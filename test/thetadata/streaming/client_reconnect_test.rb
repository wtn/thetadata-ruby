require "thetadata"
require "stringio"
require "async"

describe ThetaData::Streaming::Client do
  def fit_encode(values)
    Protocol::FPSS::FIT::Writer.new.write_changes(values)
  end

  def write_metadata_frame(stream, subscriptions = "STOCK.FREE, INDEX.PRO")
    Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::METADATA, subscriptions).write(stream)
  end

  def write_disconnect_frame(stream, reason_code)
    Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::DISCONNECTED, [reason_code].pack("S>")).write(stream)
  end

  def write_trade_frame(stream, values)
    Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE, fit_encode(values)).write(stream)
  end

  def write_req_response_frame(stream, request_id, response_code = 0)
    payload = [request_id, response_code].pack("N2")
    Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::REQ_RESPONSE, payload).write(stream)
  end

  # A stream that returns pre-written data for reads and discards writes.
  # This simulates a network socket where the server sends data we pre-loaded.
  class MockStream
    def initialize(read_data = "")
      @read_buf = StringIO.new(read_data)
      @read_buf.set_encoding("ASCII-8BIT")
      @closed = false
    end

    def read(n)
      @read_buf.read(n)
    end

    def write(data)
      data.bytesize  # discard, return bytes written
    end

    def flush; end

    def close
      @closed = true
      @read_buf.close
    end

    def closed?
      @closed
    end
  end

  # A mock endpoint that returns pre-built MockStreams on connect_async
  class MockEndpoint
    attr_reader :connect_count

    def initialize(*streams)
      @streams = streams
      @connect_count = 0
    end

    def connect_async
      stream = @streams[@connect_count]
      raise Errno::ECONNREFUSED, "mock connection refused" if stream.nil?
      @connect_count += 1
      stream
    end
  end

  def build_stream
    StringIO.new.tap {|s| s.set_encoding("ASCII-8BIT") }
  end

  def build_read_data(&block)
    buf = build_stream
    yield buf
    buf.string
  end

  def mock_stream(&block)
    data = build_read_data(&block)
    MockStream.new(data)
  end

  with "reconnection prerequisites" do
    it "handle_disconnect returns false for non-reconnectable reason" do
      stream = build_stream
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection, endpoint: MockEndpoint.new)
      client.login("test@example.com", "secret")

      event = {
        type: :disconnected,
        reason_code: ThetaData::Streaming::DisconnectReason::INVALID_CREDENTIALS,
        reconnectable: false,
      }

      result = client.send(:handle_disconnect, event)
      expect(result).to be == false
    end

    it "handle_disconnect returns false without endpoint" do
      stream = build_stream
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)
      client.login("test@example.com", "secret")

      event = { type: :disconnected, reason_code: 4, reconnectable: true }

      result = client.send(:handle_disconnect, event)
      expect(result).to be == false
    end

    it "handle_disconnect returns false without credentials" do
      stream = build_stream
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection, endpoint: MockEndpoint.new)

      event = { type: :disconnected, reason_code: 4, reconnectable: true }

      result = client.send(:handle_disconnect, event)
      expect(result).to be == false
    end

    it "handle_disconnect returns false when auto_reconnect is disabled" do
      stream = build_stream
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection, endpoint: MockEndpoint.new)
      client.login("test@example.com", "secret")
      client.auto_reconnect = false

      event = { type: :disconnected, reason_code: 4, reconnectable: true }

      result = client.send(:handle_disconnect, event)
      expect(result).to be == false
    end
  end

  with "perform_reconnect" do
    it "establishes new connection and reads from new stream" do
      initial_stream = build_stream
      reconnect_ms = mock_stream do |s|
        write_metadata_frame(s)
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(s)
      end

      endpoint = MockEndpoint.new(reconnect_ms)
      connection = ThetaData::Streaming::Connection.new(initial_stream)
      client = ThetaData::Streaming::Client.new(connection, endpoint: endpoint)
      client.login("test@example.com", "secret")

      result = client.send(:perform_reconnect, 0)

      expect(result).to be == true
      expect(endpoint.connect_count).to be == 1

      event = client.read_event
      expect(event[:type]).to be == :ping
    end

    it "re-sends login credentials (can_reconnect remains true)" do
      reconnect_ms = mock_stream do |s|
        write_metadata_frame(s)
      end

      endpoint = MockEndpoint.new(reconnect_ms)
      connection = ThetaData::Streaming::Connection.new(build_stream)
      client = ThetaData::Streaming::Client.new(connection, endpoint: endpoint)
      client.login("test@example.com", "secret")

      client.send(:perform_reconnect, 0)

      expect(!!client.can_reconnect?).to be == true
    end

    it "waits for metadata on reconnect (skips other events)" do
      reconnect_ms = mock_stream do |s|
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(s)
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(s)
        write_metadata_frame(s)
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(s)
      end

      endpoint = MockEndpoint.new(reconnect_ms)
      connection = ThetaData::Streaming::Connection.new(build_stream)
      client = ThetaData::Streaming::Client.new(connection, endpoint: endpoint)
      client.login("test@example.com", "secret")

      result = client.send(:perform_reconnect, 0)
      expect(result).to be == true

      event = client.read_event
      expect(event[:type]).to be == :ping
    end

    it "returns false when connect_async fails" do
      endpoint = MockEndpoint.new  # no streams = will raise

      connection = ThetaData::Streaming::Connection.new(build_stream)
      client = ThetaData::Streaming::Client.new(connection, endpoint: endpoint)
      client.login("test@example.com", "secret")

      result = client.send(:perform_reconnect, 0)
      expect(result).to be == false
    end

    it "exposes the underlying error via #last_reconnect_error when reconnect fails" do
      endpoint = MockEndpoint.new  # no streams = connect_async raises Errno::ECONNREFUSED

      connection = ThetaData::Streaming::Connection.new(build_stream)
      client = ThetaData::Streaming::Client.new(connection, endpoint: endpoint)
      client.login("test@example.com", "secret")

      expect(client.last_reconnect_error).to be == nil

      client.send(:perform_reconnect, 0)

      expect(client.last_reconnect_error).to be_a(Errno::ECONNREFUSED)
      expect(client.last_reconnect_error.message).to be =~ /mock connection refused/
    end

    it "clears #last_reconnect_error after a successful reconnect" do
      reconnect_ms = mock_stream {|s| write_metadata_frame(s) }
      endpoint = MockEndpoint.new(reconnect_ms)

      connection = ThetaData::Streaming::Connection.new(build_stream)
      client = ThetaData::Streaming::Client.new(connection, endpoint: endpoint)
      client.login("test@example.com", "secret")

      # Seed a stale error to confirm a successful reconnect resets it.
      client.instance_variable_set(:@last_reconnect_error, RuntimeError.new("stale"))

      result = client.send(:perform_reconnect, 0)
      expect(result).to be == true
      expect(client.last_reconnect_error).to be == nil
    end

    it "wait_for_metadata_on_reconnect returns on disconnect frame" do
      reconnect_ms = mock_stream do |s|
        write_disconnect_frame(s, ThetaData::Streaming::DisconnectReason::INVALID_CREDENTIALS)
      end

      endpoint = MockEndpoint.new(reconnect_ms)
      connection = ThetaData::Streaming::Connection.new(build_stream)
      client = ThetaData::Streaming::Client.new(connection, endpoint: endpoint)
      client.login("test@example.com", "secret")

      result = client.send(:perform_reconnect, 0)
      expect(result).to be == true
    end

    it "wait_for_metadata_on_reconnect returns on EOF" do
      reconnect_ms = mock_stream {|s| }  # empty

      endpoint = MockEndpoint.new(reconnect_ms)
      connection = ThetaData::Streaming::Connection.new(build_stream)
      client = ThetaData::Streaming::Client.new(connection, endpoint: endpoint)
      client.login("test@example.com", "secret")

      result = client.send(:perform_reconnect, 0)
      expect(result).to be == true
    end
  end

  with "resubscribe_all" do
    it "re-sends all active trade and quote subscriptions" do
      reconnect_ms = mock_stream do |s|
        write_metadata_frame(s)
      end

      endpoint = MockEndpoint.new(reconnect_ms)
      connection = ThetaData::Streaming::Connection.new(build_stream)
      client = ThetaData::Streaming::Client.new(connection, endpoint: endpoint)
      client.login("test@example.com", "secret")

      contract_spx = ThetaData::Streaming::Contract.index("SPX")
      contract_vix = ThetaData::Streaming::Contract.index("VIX")
      client.subscribe_trade(contract_spx)
      client.subscribe_trade(contract_vix)
      client.subscribe_quote(contract_spx)

      client.send(:perform_reconnect, 0)

      active_trades = client.instance_variable_get(:@active_trades)
      active_quotes = client.instance_variable_get(:@active_quotes)

      expect(active_trades.size).to be == 2
      expect(active_quotes.size).to be == 1
    end
  end

  with "resubscribe_all with full subs" do
    it "re-sends full type subscriptions on reconnect" do
      reconnect_ms = mock_stream do |s|
        write_metadata_frame(s)
      end

      endpoint = MockEndpoint.new(reconnect_ms)
      connection = ThetaData::Streaming::Connection.new(build_stream)
      client = ThetaData::Streaming::Client.new(connection, endpoint: endpoint)
      client.login("test@example.com", "secret")

      client.subscribe_full_trades(ThetaData::Streaming::SecType::STOCK)
      client.subscribe_full_quotes(ThetaData::Streaming::SecType::OPTION)

      client.send(:perform_reconnect, 0)

      active_full = client.instance_variable_get(:@active_full_subs)
      expect(active_full.size).to be == 2
    end
  end

  with "each_event with disconnect" do
    it "yields disconnect event for non-reconnectable reason" do
      stream = build_stream
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(stream)
      write_disconnect_frame(stream, ThetaData::Streaming::DisconnectReason::INVALID_CREDENTIALS)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      events = []
      client.each_event do |e|
        events << e[:type]
        break if e[:type] == :disconnected
      end

      expect(events).to be == [:ping, :disconnected]
    end

    it "reconnects and yields :reconnected for reconnectable disconnect" do
      initial_stream = mock_stream do |s|
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(s)
        write_disconnect_frame(s, ThetaData::Streaming::DisconnectReason::SERVER_RESTARTING)
      end

      reconnect_ms = mock_stream do |s|
        write_metadata_frame(s)
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(s)
      end

      endpoint = MockEndpoint.new(reconnect_ms)
      connection = ThetaData::Streaming::Connection.new(initial_stream)
      client = ThetaData::Streaming::Client.new(connection, endpoint: endpoint)
      client.login("test@example.com", "secret")

      events = []
      client.each_event do |e|
        events << e[:type]
        break if events.length >= 4
      end

      expect(events).to be == [:ping, :disconnected, :reconnected, :ping]
    end
  end
end
