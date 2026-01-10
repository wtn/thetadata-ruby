require "async"
require "async/grpc"
require "async/http/internet"
require "async/semaphore"
require "json"
require "zstd-ruby"
require "bigdecimal"

module ThetaData
  module REST
    class Connection
      SERVICE_NAME = "Endpoints.ThetaTerminal"

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

        self
      end

      def call(method_name, request)
        ensure_authenticated!

        Sync do
          semaphore.acquire do
            invoke_with_retry(method_name, request)
          end
        end
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
        concurrency = Configuration::TIER_CONCURRENCY[tier] || 1
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
          Time.at(data_value.timestamp.epoch_ms / 1000.0)
        when nil
          nil
        else
          raise ServerError, "Unknown data type: #{data_value.data_type.inspect}"
        end
      end

      def decode_price(raw_value, price_type)
        return nil if raw_value.nil?

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

      def merge_responses(responses)
        return { headers: [], rows: [] } if responses.empty?

        headers = responses.first[:headers]
        rows = responses.flat_map { |r| r[:rows] }

        { headers: headers, rows: rows }
      end
    end
  end
end
