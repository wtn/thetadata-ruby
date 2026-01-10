require "async"
require "async/grpc"
require "async/http/internet"
require "async/semaphore"
require "json"
require "zstd-ruby"
require "bigdecimal"
require "tzinfo"

module ThetaData
  module REST
    class Connection
      # Wire path the MDDS server routes on (differs from our proto package name)
      SERVICE_NAME = "BetaEndpoints.BetaThetaTerminal"

      # Theta Data's wire protocol uses NY market time as zone 0 (the proto3 default),
      # so we keep one shared TZInfo lookup for the hot path.
      NY_TZ = TZInfo::Timezone.get("America/New_York")

      MAX_RETRIES = 3
      BASE_DELAY = 0.5
      MAX_DELAY = 8.0

      TRANSIENT_ERRORS = [
        Protocol::GRPC::Unavailable,
        Protocol::GRPC::DeadlineExceeded,
        Protocol::GRPC::Internal,
        EOFError,
        IOError,
        SocketError,
        Errno::ECONNRESET,
        Errno::ECONNREFUSED,
        Errno::ETIMEDOUT,
      ].freeze

      attr_reader :email, :password, :session

      def initialize(email:, password:)
        @email = email
        @password = password
        @session = nil
        @grpc_client = nil
        @internet = nil
        @semaphore = nil
      end

      def authenticated?
        @session&.valid? || false
      end

      def authenticate!
        config = ThetaData.configuration

        Sync do
          Async::Task.current.with_timeout(config.auth_timeout) do
            response = internet.post(
              config.auth_url,
              auth_headers,
              JSON.generate(email: @email, password: @password),
            )

            status = response.status
            body = response.read

            case status
            when 200
              data = JSON.parse(body, symbolize_names: true)
              @session = Session.new(
                session_id: data[:sessionId],
                user: data[:user],
              )
              update_max_concurrency!
            when 401, 403
              raise AuthenticationError, "Invalid credentials"
            else
              raise ServerError.new("Authentication failed: #{status}", grpc_status: status)
            end
          end
        end

        self
      rescue Async::TimeoutError
        raise ThetaData::TimeoutError, "Authentication timed out after #{config.auth_timeout}s"
      end

      def call(method_name, request)
        ensure_authenticated!

        Sync do
          semaphore.acquire do
            invoke_with_retry(method_name, request)
          end
        end
      rescue Protocol::GRPC::NotFound => e
        raise NotFoundError, e.message
      rescue Protocol::GRPC::Unauthenticated => e
        raise AuthenticationError, e.message
      rescue Protocol::GRPC::Error => e
        raise_for_grpc_error! e
      rescue EOFError, IOError, SocketError,
             Errno::ECONNRESET, Errno::ECONNREFUSED, Errno::ETIMEDOUT => e
        raise ConnectionError, e.message
      end

      # Match Python's wire shape (only auth_token populated) but also flag the
      # call as Ruby + gem version so the server can distinguish our traffic.
      CLIENT_TYPE = "thetadata-ruby-#{ThetaData::VERSION}".freeze

      def query_info
        ::Endpoints::QueryInfo.new(
          auth_token: ::Endpoints::AuthToken.new(session_uuid: @session.session_id),
          client_type: CLIENT_TYPE,
        )
      end

      def close
        @grpc_client&.close
        @grpc_client = nil
        @internet&.close
        @internet = nil
      end

      private

      def grpc_client
        @grpc_client ||= begin
          config = ThetaData.configuration
          endpoint = "https://#{config.mdds_host}:#{config.mdds_port}"
          Async::GRPC::Client.open(endpoint)
        end
      end

      def interface
        @interface ||= Proto::ThetaTerminalInterface.new(SERVICE_NAME)
      end

      def internet
        @internet ||= Async::HTTP::Internet.new
      end

      def semaphore
        @semaphore ||= Async::Semaphore.new(ThetaData.configuration.max_concurrency)
      end

      def auth_headers
        config = ThetaData.configuration
        [
          ["content-type", "application/json"],
          ["accept", "application/json"],
          ["td-terminal-key", config.terminal_key],
        ]
      end

      def ensure_authenticated!
        return if authenticated?

        authenticate!
      end

      def invoke_with_retry(method_name, request)
        attempt = 0

        begin
          attempt += 1
          invoke_grpc(method_name, request)
        rescue Protocol::GRPC::Unauthenticated
          raise if attempt > 1

          invalidate_session!
          authenticate!
          retry
        rescue *TRANSIENT_ERRORS
          raise if attempt >= MAX_RETRIES

          sleep_with_backoff(attempt) if attempt > 1
          reset_grpc_client!
          retry
        end
      end

      def invoke_grpc(method_name, request)
        responses = []

        grpc_client.invoke(interface, method_name, request) do |response_data|
          responses << parse_response_data(response_data)
        end

        merge_responses(responses)
      end

      def sleep_with_backoff(attempt)
        delay = [BASE_DELAY * (2 ** (attempt - 2)), MAX_DELAY].min
        jitter = rand * 0.5 * delay
        sleep(delay + jitter)
      end

      def invalidate_session!
        @session = nil
      end

      def update_max_concurrency!
        tier = @session.subscription_tier.downcase.to_sym
        concurrency = Configuration::TIER_CONCURRENCY[tier]
        if concurrency.nil?
          warn "ThetaData: unknown subscription tier #{tier.inspect} -- defaulting concurrency to 1. Known tiers: #{Configuration::TIER_CONCURRENCY.keys.inspect}"
          concurrency = 1
        end
        ThetaData.configuration.max_concurrency = concurrency
      end

      def reset_grpc_client!
        @grpc_client&.close
        @grpc_client = nil
      end

      def parse_response_data(response_data)
        compressed_data = response_data.compressed_data
        compression_desc = response_data.compression_description
        algo = compression_desc&.algo

        decompressed = if algo == :ZSTD
          Zstd.decompress(compressed_data)
        else
          compressed_data
        end

        parse_data_table(::Endpoints::DataTable.decode(decompressed))
      end

      def parse_data_table(data_table)
        headers = data_table.headers.map(&:to_s)
        rows = data_table.data_table.map do |row|
          row.values.map { |v| extract_value(v) }
        end

        { headers: headers, rows: rows }
      end

      def extract_value(data_value)
        case data_value.data_type
        when :text
          data_value.text
        when :number
          data_value.number
        when :price
          decode_price(data_value.price.value, data_value.price.type)
        when :timestamp
          decode_timestamp(data_value.timestamp)
        when :null_value, nil
          nil
        else
          raise ServerError, "Unknown data type: #{data_value.data_type.inspect}"
        end
      end

      # ZonedDateTime carries an enum (NEW_YORK = 0, UTC = 1). Match the Python
      # client: NEW_YORK -> Time in America/New_York; otherwise UTC.
      def decode_timestamp(zdt)
        ms = zdt.epoch_ms
        tz = (zdt.zone == :NEW_YORK) ? NY_TZ : "UTC"
        Time.at(ms / 1000, ms % 1000, :millisecond, in: tz)
      end

      # Type 0 signals "no data" in the wire protocol (Python returns NaN here).
      # We return nil so it round-trips cleanly through Ruby.
      def decode_price(raw_value, price_type)
        return nil if raw_value.nil? || price_type == 0

        case price_type
        when 10 then BigDecimal(raw_value)
        when 8  then BigDecimal(raw_value) / 100
        when 7  then BigDecimal(raw_value) / 1_000
        when 6  then BigDecimal(raw_value) / 10_000
        else
          if price_type > 10
            BigDecimal(raw_value) * (10 ** (price_type - 10))
          else
            BigDecimal(raw_value) / (10 ** (10 - price_type))
          end
        end
      end

      def raise_for_grpc_error!(error)
        case error.status_code
        when 7  # PERMISSION_DENIED
          raise SubscriptionError, error.message
        when 4, 14  # DEADLINE_EXCEEDED, UNAVAILABLE
          raise ConnectionError, error.message
        when 8  # RESOURCE_EXHAUSTED
          raise RateLimitError, error.message
        else
          raise ServerError.new(error.message, grpc_status: error.status_code)
        end
      end

      def merge_responses(responses)
        return { headers: [], rows: [] } if responses.empty?

        headers = responses.first[:headers]
        rows = responses.flat_map { |r| r[:rows] }

        { headers: headers, rows: rows }
      end
    end
  end
end
