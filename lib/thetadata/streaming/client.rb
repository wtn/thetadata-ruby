require "async"

module ThetaData
  module Streaming
    MessageType = Protocol::FPSS::MessageType
    ResponseType = Protocol::FPSS::ResponseType

    class Client
      RECONNECT_DELAY = 2.0
      RECONNECT_DELAY_TOO_MANY_REQUESTS = 130.0
      PING_INTERVAL = 5.0

      attr_reader :connection, :subscriptions, :endpoint, :last_reconnect_error
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
        @active_full_subs = {}
        @auto_reconnect = true
        @ping_task = nil
        @last_reconnect_error = nil
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
        stop_pinger
        @connection.close
      end

      def login(email, password)
        @credentials = { email: email, password: password }
        send_login(email, password)
      end

      def subscribe_trade(contract)
        next_request_id.tap do |request_id|
          @active_trades[request_id] = contract
          send_subscribe(MessageType::TRADE, request_id, contract)
        end
      end

      def subscribe_quote(contract)
        next_request_id.tap do |request_id|
          @active_quotes[request_id] = contract
          send_subscribe(MessageType::QUOTE, request_id, contract)
        end
      end

      FIREHOSE_SEC_TYPES = [SecType::STOCK, SecType::OPTION].freeze

      def subscribe_full_trades(sec_type)
        validate_firehose_sec_type!(sec_type)
        next_request_id.tap do |request_id|
          @active_full_subs[request_id] = { kind: :trade, sec_type: sec_type }
          send_full_subscribe(MessageType::TRADE, request_id, sec_type)
        end
      end

      def subscribe_full_quotes(sec_type)
        validate_firehose_sec_type!(sec_type)
        next_request_id.tap do |request_id|
          @active_full_subs[request_id] = { kind: :quote, sec_type: sec_type }
          send_full_subscribe(MessageType::QUOTE, request_id, sec_type)
        end
      end

      def subscribe_full_open_interest(sec_type)
        validate_firehose_sec_type!(sec_type)
        next_request_id.tap do |request_id|
          @active_full_subs[request_id] = { kind: :open_interest, sec_type: sec_type }
          send_full_subscribe(MessageType::OPEN_INTEREST, request_id, sec_type)
        end
      end

      def unsubscribe_trade(request_id)
        contract = @active_trades.delete(request_id)
        return nil unless contract

        next_request_id.tap do |new_id|
          send_unsubscribe(MessageType::REMOVE_TRADE, new_id, contract)
        end
      end

      def unsubscribe_quote(request_id)
        contract = @active_quotes.delete(request_id)
        return nil unless contract

        next_request_id.tap do |new_id|
          send_unsubscribe(MessageType::REMOVE_QUOTE, new_id, contract)
        end
      end

      def unsubscribe_full_trades(request_id)
        sub = @active_full_subs.delete(request_id)
        return nil unless sub

        next_request_id.tap do |new_id|
          send_full_subscribe(MessageType::REMOVE_TRADE, new_id, sub[:sec_type])
        end
      end

      def unsubscribe_full_quotes(request_id)
        sub = @active_full_subs.delete(request_id)
        return nil unless sub

        next_request_id.tap do |new_id|
          send_full_subscribe(MessageType::REMOVE_QUOTE, new_id, sub[:sec_type])
        end
      end

      def unsubscribe_full_open_interest(request_id)
        sub = @active_full_subs.delete(request_id)
        return nil unless sub

        next_request_id.tap do |new_id|
          send_full_subscribe(MessageType::REMOVE_OPEN_INTEREST, new_id, sub[:sec_type])
        end
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

      def start_pinger
        stop_pinger
        return unless Async::Task.current?

        @ping_task = Async::Task.current.async(transient: true) do |task|
          loop do
            task.sleep(PING_INTERVAL)
            break if closed?
            ping
          end
        rescue IOError, Errno::ECONNRESET
          # connection lost, pinger exits silently
        end
      end

      def stop_pinger
        @ping_task&.stop
        @ping_task = nil
      end

      def handle_disconnect(event)
        stop_pinger
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
        start_pinger
        @reconnecting = true
        @last_reconnect_error = nil
        true
      rescue => e
        # Don't raise -- it would kill the read loop in `each_event`. Instead
        # capture the error so callers (and the failure-path test) can see why.
        @last_reconnect_error = e
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

      def next_request_id
        @request_id_counter += 1
      end

      def resubscribe_all
        @active_trades.each_value do |contract|
          send_subscribe(MessageType::TRADE, next_request_id, contract)
        end

        @active_quotes.each_value do |contract|
          send_subscribe(MessageType::QUOTE, next_request_id, contract)
        end

        @active_full_subs.each_value do |sub|
          code = FULL_SUB_KIND_TO_CODE.fetch(sub[:kind])
          send_full_subscribe(code, next_request_id, sub[:sec_type])
        end
      end

      FULL_SUB_KIND_TO_CODE = {
        trade: MessageType::TRADE,
        quote: MessageType::QUOTE,
        open_interest: MessageType::OPEN_INTEREST,
      }.freeze

      def send_login(email, password)
        data = [0, email.bytesize].pack("CS>") << email << password
        @connection.write_frame(MessageType::CREDENTIALS, data)
      end

      def send_subscribe(code, request_id, contract)
        @connection.write_frame(code, [request_id].pack("N") << contract.to_bytes)
      end

      def send_unsubscribe(code, request_id, contract)
        @connection.write_frame(code, [request_id].pack("N") << contract.to_bytes)
      end

      def validate_firehose_sec_type!(sec_type)
        return if FIREHOSE_SEC_TYPES.include?(sec_type)

        name = SecType.name(sec_type) || sec_type
        raise ArgumentError, "firehose subscriptions only support stock and option, got #{name}"
      end

      def send_full_subscribe(code, request_id, sec_type)
        @connection.write_frame(code, [request_id, sec_type].pack("Nc"))
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
          start_pinger
          { type: :metadata, subscriptions: @subscriptions }
        when MessageType::CONTRACT
          parse_contract_event(frame.payload)
        when MessageType::DISCONNECTED
          reason_code = frame.payload&.unpack1("s>")
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
        contract = Contract.from_bytes(data[4..])

        {
          type: :contract,
          contract_id: contract_id,
          contract: contract,
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
