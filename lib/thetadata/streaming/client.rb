require "async"

module ThetaData
  module Streaming
    MessageType = Protocol::FPSS::MessageType
    ResponseType = Protocol::FPSS::ResponseType

    class Client
      RECONNECT_DELAY = 2.0
      RECONNECT_DELAY_TOO_MANY_REQUESTS = 130.0

      attr_reader :connection, :subscriptions, :endpoint
      attr_accessor :auto_reconnect

      def initialize(connection, endpoint: nil)
        @connection = connection
        @endpoint = endpoint
        @request_id_counter = 0
        @fit_reader = Protocol::FPSS::FIT::Reader.new
        @ticks = {}
        @quotes = {}
        @subscriptions = nil
        @credentials = nil
        @active_trades = {}
        @active_quotes = {}
        @auto_reconnect = true
      end

      def self.open_async(endpoint)
        stream = endpoint.connect_async
        connection = Connection.new(stream)
        new(connection, endpoint: endpoint)
      end

      def self.open(endpoint, &block)
        if block_given?
          Sync do
            client = open_async(endpoint)
            begin
              yield client
            ensure
              client.close
            end
          end
        else
          Sync do
            open_async(endpoint)
          end
        end
      end

      def closed?
        @connection.closed?
      end

      def close
        @auto_reconnect = false
        @connection.close
      end

      def login(email, password)
        @credentials = { email: email, password: password }
        send_login(email, password)
      end

      def subscribe_trade(contract)
        @request_id_counter += 1
        request_id = @request_id_counter
        @active_trades[request_id] = contract

        send_subscribe_trade(request_id, contract)
        request_id
      end

      def subscribe_quote(contract)
        @request_id_counter += 1
        request_id = @request_id_counter
        @active_quotes[request_id] = contract

        send_subscribe_quote(request_id, contract)
        request_id
      end

      def ping
        @connection.write_frame(MessageType::PING, "\x00")
        @connection.flush
      end

      def read_event
        frame = @connection.read_frame
        return nil if frame.nil?

        parse_frame(frame)
      end

      def each_event
        return enum_for(:each_event) unless block_given?

        loop do
          until closed?
            event = read_event
            break if event.nil?

            if event[:type] == :disconnected
              yield event
              if handle_disconnect(event)
                yield({ type: :reconnected })
              end
              break
            end

            yield event
          end

          break unless @reconnecting
          @reconnecting = false
        end
      end

      def reconnect!
        return false unless can_reconnect?

        perform_reconnect(RECONNECT_DELAY)
      end

      def can_reconnect?
        @auto_reconnect && @endpoint && @credentials
      end

      private

      def handle_disconnect(event)
        return false unless event[:reconnectable]
        return false unless can_reconnect?

        delay = event[:reason_code] == DisconnectReason::TOO_MANY_REQUESTS ?
          RECONNECT_DELAY_TOO_MANY_REQUESTS : RECONNECT_DELAY

        perform_reconnect(delay)
      end

      def perform_reconnect(delay)
        sleep(delay)

        stream = @endpoint.connect_async
        @connection = Connection.new(stream)

        send_login(@credentials[:email], @credentials[:password])
        wait_for_metadata_on_reconnect

        resubscribe_all
        @reconnecting = true
        true
      rescue => e
        false
      end

      def wait_for_metadata_on_reconnect
        loop do
          event = read_event
          return if event.nil?
          return if event[:type] == :metadata
          return if event[:type] == :disconnected
        end
      end

      def resubscribe_all
        @active_trades.each_value do |contract|
          @request_id_counter += 1
          send_subscribe_trade(@request_id_counter, contract)
        end

        @active_quotes.each_value do |contract|
          @request_id_counter += 1
          send_subscribe_quote(@request_id_counter, contract)
        end
      end

      def send_login(email, password)
        data = [0, email.bytesize].pack("CS>") << email << password
        @connection.write_frame(MessageType::CREDENTIALS, data)
      end

      def send_subscribe_trade(request_id, contract)
        @connection.write_frame(MessageType::TRADE, [request_id].pack("N") << contract.to_bytes)
      end

      def send_subscribe_quote(request_id, contract)
        @connection.write_frame(MessageType::QUOTE, [request_id].pack("N") << contract.to_bytes)
      end

      def parse_frame(frame)
        case frame.type
        when MessageType::TRADE
          parse_trade_event(frame.payload)
        when MessageType::QUOTE
          parse_quote_event(frame.payload)
        when MessageType::OHLCVC
          parse_ohlcvc_event(frame.payload)
        when MessageType::REQ_RESPONSE
          parse_req_response(frame.payload)
        when MessageType::METADATA
          @subscriptions = parse_subscriptions(frame.payload)
          { type: :metadata, subscriptions: @subscriptions }
        when MessageType::CONTRACT
          parse_contract_event(frame.payload)
        when MessageType::DISCONNECTED
          reason_code = frame.payload&.unpack1("S>")
          {
            type: :disconnected,
            reason_code: reason_code,
            reason: DisconnectReason.name(reason_code),
            reconnectable: DisconnectReason.reconnectable?(reason_code),
          }
        when MessageType::ERROR
          { type: :error, message: frame.payload }
        when MessageType::START
          { type: :start }
        when MessageType::STOP
          { type: :stop }
        when MessageType::PING
          { type: :ping }
        else
          { type: MessageType.name(frame.type), code: frame.type, data: frame.payload }
        end
      end

      def parse_trade_event(data)
        @fit_reader.open(data)
        changes = @fit_reader.read_changes
        return nil unless changes

        contract_id = changes[0]
        @ticks[contract_id] ||= TradeTick.new
        @ticks[contract_id].apply_changes(changes)

        {
          type: :trade,
          contract_id: contract_id,
          tick: @ticks[contract_id].dup,
          raw_changes: changes,
        }
      end

      def parse_quote_event(data)
        @fit_reader.open(data)
        changes = @fit_reader.read_changes
        return nil unless changes

        contract_id = changes[0]
        @quotes[contract_id] ||= QuoteTick.new
        @quotes[contract_id].apply_changes(changes)

        {
          type: :quote,
          contract_id: contract_id,
          quote: @quotes[contract_id].dup,
          raw_changes: changes,
        }
      end

      def parse_ohlcvc_event(data)
        @fit_reader.open(data)
        changes = @fit_reader.read_changes
        return nil unless changes

        {
          type: :ohlcvc,
          contract_id: changes[0],
          ms_of_day: changes[1],
          open: changes[2],
          high: changes[3],
          low: changes[4],
          close: changes[5],
          volume: changes[6],
          count: changes[7],
          price_type: changes[8],
          date: changes[9],
        }
      end

      def parse_req_response(data)
        req_id = data.unpack1("N")
        response_code = data[4..7].unpack1("N")

        {
          type: :req_response,
          request_id: req_id,
          response: ResponseType.name(response_code) || :unknown,
          response_code: response_code,
        }
      end

      def parse_contract_event(data)
        contract_id = data[0..3].unpack1("N")

        {
          type: :contract,
          contract_id: contract_id,
          data: data[4..],
        }
      end

      def parse_subscriptions(data)
        data.split(", ").each_with_object({}) do |entry, hash|
          type, level = entry.split(".")
          next unless type && level
          hash[type.downcase.to_sym] = level.downcase.to_sym
        end
      end
    end
  end
end
