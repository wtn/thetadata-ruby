require "thetadata"
require "stringio"
require "async"

describe ThetaData::Streaming::Client do
  def mock_connection
    stream = StringIO.new
    ThetaData::Streaming::Connection.new(stream)
  end

  with "initialization" do
    it "wraps a connection" do
      connection = mock_connection
      client = ThetaData::Streaming::Client.new(connection)

      expect(client.connection).to be == connection
    end
  end

  with "#closed?" do
    it "delegates to connection" do
      connection = mock_connection
      client = ThetaData::Streaming::Client.new(connection)

      expect(client.closed?).to be == false

      connection.close
      expect(client.closed?).to be == true
    end
  end

  with "#close" do
    it "closes the connection" do
      connection = mock_connection
      client = ThetaData::Streaming::Client.new(connection)

      client.close

      expect(connection.closed?).to be == true
    end
  end

  with "#login" do
    it "sends credentials frame" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      client.login("test@example.com", "secret123")

      stream.rewind
      frame = Protocol::FPSS::Frame.read(stream)

      expect(frame.type).to be == Protocol::FPSS::MessageType::CREDENTIALS
      # Verify payload format: [version:1][email_length:2][email][password]
      payload = frame.payload
      version = payload.unpack1("C")
      email_len = payload[1..2].unpack1("S>")
      email = payload[3, email_len]
      password = payload[(3 + email_len)..]

      expect(version).to be == 0
      expect(email).to be == "test@example.com"
      expect(password).to be == "secret123"
    end
  end

  with "#subscribe_trade" do
    it "sends trade subscription frame and returns request_id" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      contract = ThetaData::Streaming::Contract.index("SPX")
      request_id = client.subscribe_trade(contract)

      expect(request_id).to be == 1

      stream.rewind
      frame = Protocol::FPSS::Frame.read(stream)

      expect(frame.type).to be == Protocol::FPSS::MessageType::TRADE
    end

    it "increments request_id for each subscription" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      contract = ThetaData::Streaming::Contract.index("SPX")

      id1 = client.subscribe_trade(contract)
      id2 = client.subscribe_trade(contract)
      id3 = client.subscribe_quote(contract)

      expect(id1).to be == 1
      expect(id2).to be == 2
      expect(id3).to be == 3
    end
  end

  with "#subscribe_quote" do
    it "sends quote subscription frame" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      contract = ThetaData::Streaming::Contract.index("SPX")
      client.subscribe_quote(contract)

      stream.rewind
      frame = Protocol::FPSS::Frame.read(stream)

      expect(frame.type).to be == Protocol::FPSS::MessageType::QUOTE
    end
  end

  with "#ping" do
    it "sends ping frame" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      client.ping

      stream.rewind
      frame = Protocol::FPSS::Frame.read(stream)

      expect(frame.type).to be == Protocol::FPSS::MessageType::PING
    end
  end

  with "#read_event" do
    it "parses trade event with FIT delta accumulation" do
      stream = StringIO.new
      # Write a trade frame with FIT-encoded data
      # contract_id=1802875, then end: 0x18 0x02 0x87 0x5D
      fit_data = "\x18\x02\x87\x5D"
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE, fit_data).write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:type]).to be == :trade
      expect(event[:contract_id]).to be == 1802875
      expect(event[:tick]).to be_a(ThetaData::Streaming::TradeTick)
    end

    it "parses quote event" do
      stream = StringIO.new
      fit_data = "\x18\x02\x87\x5D"
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::QUOTE, fit_data).write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:type]).to be == :quote
      expect(event[:contract_id]).to be == 1802875
    end

    it "parses metadata event" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::METADATA, "STOCK.FREE, INDEX.PRO").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:type]).to be == :metadata
      expect(event[:subscriptions]).to be == { stock: :free, index: :pro }
      expect(client.subscriptions).to be == { stock: :free, index: :pro }
    end

    it "parses ping event" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:type]).to be == :ping
    end

    it "parses req_response event" do
      stream = StringIO.new
      # req_id=1, response_code=0 (subscribed)
      payload = "\x00\x00\x00\x01\x00\x00\x00\x00"
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::REQ_RESPONSE, payload).write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:type]).to be == :req_response
      expect(event[:request_id]).to be == 1
      expect(event[:response]).to be == :subscribed
    end

    it "returns nil on EOF" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      expect(client.read_event).to be == nil
    end

    it "parses error event" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::ERROR, "Something went wrong").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:type]).to be == :error
      expect(event[:message]).to be == "Something went wrong"
    end

    it "parses disconnected event with reason details" do
      stream = StringIO.new
      # reason_code 6 = ACCOUNT_ALREADY_CONNECTED
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::DISCONNECTED, "\x00\x06").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:type]).to be == :disconnected
      expect(event[:reason_code]).to be == 6
      expect(event[:reason]).to be == "ACCOUNT_ALREADY_CONNECTED"
      expect(event[:reconnectable]).to be == false
    end

    it "marks SERVER_RESTARTING as reconnectable" do
      stream = StringIO.new
      # reason_code 15 = SERVER_RESTARTING
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::DISCONNECTED, "\x00\x0F").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:type]).to be == :disconnected
      expect(event[:reason_code]).to be == 15
      expect(event[:reason]).to be == "SERVER_RESTARTING"
      expect(event[:reconnectable]).to be == true
    end

    it "parses start event" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::START, "").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:type]).to be == :start
    end

    it "parses stop event" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::STOP, "").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:type]).to be == :stop
    end

    it "parses contract event" do
      stream = StringIO.new
      # contract_id (4 bytes) + extra data
      payload = "\x00\x1B\x7F\x4B" + "contract_data"
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::CONTRACT, payload).write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:type]).to be == :contract
      expect(event[:contract_id]).to be == 1802059
      expect(event[:data]).to be == "contract_data"
    end

    it "parses all req_response codes" do
      codes = {
        0 => :subscribed,
        1 => :error,
        2 => :max_streams_reached,
        3 => :invalid_perms,
      }

      codes.each do |code, expected_response|
        stream = StringIO.new
        payload = "\x00\x00\x00\x01" + [code].pack("N")
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::REQ_RESPONSE, payload).write(stream)
        stream.rewind

        connection = ThetaData::Streaming::Connection.new(stream)
        client = ThetaData::Streaming::Client.new(connection)

        event = client.read_event

        expect(event[:response]).to be == expected_response
      end
    end

    it "handles unknown message type" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(99, "unknown_data").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:code]).to be == 99
      expect(event[:data]).to be == "unknown_data"
    end
  end

  with "FIT delta accumulation" do
    it "accumulates trade deltas across multiple events" do
      stream = StringIO.new
      # First trade: contract_id=100, ms_of_day=1000, price=55000, price_type=8
      # [100, 1000, 0, 0, 0, 55000, 10, 8, 20250110]
      fit_data1 = "\x10\x0B\x10\x00\xD0\x00\x00\x00\xB5\x50\x00\x01\x0B\x80\x22\x00\x12\x50\x11\x0D"
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE, fit_data1).write(stream)

      stream.rewind
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event1 = client.read_event
      expect(event1[:type]).to be == :trade
      expect(event1[:tick]).to be_a(ThetaData::Streaming::TradeTick)
    end

    it "maintains separate state for different contract_ids" do
      stream = StringIO.new
      # Two different contracts
      fit_data1 = "\x10\x0D"  # contract_id=100
      fit_data2 = "\x20\x0D"  # contract_id=200
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE, fit_data1).write(stream)
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE, fit_data2).write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event1 = client.read_event
      event2 = client.read_event

      expect(event1[:contract_id]).to be == 100
      expect(event2[:contract_id]).to be == 200
    end
  end

  with "#each_event" do
    it "yields events until EOF" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(stream)
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      events = []
      client.each_event { |e| events << e }

      expect(events.length).to be == 2
    end

    it "returns enumerator without block" do
      connection = mock_connection
      client = ThetaData::Streaming::Client.new(connection)

      expect(client.each_event).to be_a(Enumerator)
    end
  end

  with "auto_reconnect" do
    it "defaults to true" do
      connection = mock_connection
      client = ThetaData::Streaming::Client.new(connection)

      expect(client.auto_reconnect).to be == true
    end

    it "can be disabled" do
      connection = mock_connection
      client = ThetaData::Streaming::Client.new(connection)
      client.auto_reconnect = false

      expect(client.auto_reconnect).to be == false
    end

    it "is disabled when close is called" do
      connection = mock_connection
      client = ThetaData::Streaming::Client.new(connection)
      client.close

      expect(client.auto_reconnect).to be == false
    end
  end

  with "#can_reconnect?" do
    it "returns falsy without endpoint" do
      connection = mock_connection
      client = ThetaData::Streaming::Client.new(connection)

      expect(!!client.can_reconnect?).to be == false
    end

    it "returns falsy without credentials" do
      connection = mock_connection
      client = ThetaData::Streaming::Client.new(connection, endpoint: Object.new)

      expect(!!client.can_reconnect?).to be == false
    end

    it "returns falsy when auto_reconnect is disabled" do
      connection = mock_connection
      client = ThetaData::Streaming::Client.new(connection, endpoint: Object.new)
      client.login("test@example.com", "secret")
      client.auto_reconnect = false

      expect(!!client.can_reconnect?).to be == false
    end

    it "returns truthy with endpoint and credentials" do
      connection = mock_connection
      client = ThetaData::Streaming::Client.new(connection, endpoint: Object.new)
      client.login("test@example.com", "secret")

      expect(!!client.can_reconnect?).to be == true
    end
  end

  with "#login" do
    it "stores credentials for reconnection" do
      connection = mock_connection
      client = ThetaData::Streaming::Client.new(connection, endpoint: Object.new)

      expect(!!client.can_reconnect?).to be == false

      client.login("test@example.com", "secret")

      expect(!!client.can_reconnect?).to be == true
    end
  end

  with "constants" do
    it "has RECONNECT_DELAY of 2.0 seconds" do
      expect(ThetaData::Streaming::Client::RECONNECT_DELAY).to be == 2.0
    end

    it "has RECONNECT_DELAY_TOO_MANY_REQUESTS of 130.0 seconds" do
      expect(ThetaData::Streaming::Client::RECONNECT_DELAY_TOO_MANY_REQUESTS).to be == 130.0
    end
  end

  with ".open" do
    it "creates client from endpoint and yields" do
      skip_unless_integration!

      endpoint = ThetaData::Streaming::Endpoint.default
      yielded_client = nil

      ThetaData::Streaming::Client.open(endpoint) do |client|
        yielded_client = client
        expect(client).to be_a(ThetaData::Streaming::Client)
        expect(client).not.to be(:closed?)
      end

      expect(yielded_client).to be(:closed?)
    end

    it "returns client without block" do
      skip_unless_integration!

      endpoint = ThetaData::Streaming::Endpoint.default
      client = ThetaData::Streaming::Client.open(endpoint)

      expect(client).to be_a(ThetaData::Streaming::Client)
      expect(client).not.to be(:closed?)
    ensure
      client&.close
    end
  end

  with ".open_async" do
    it "creates client from endpoint within Sync block" do
      skip_unless_integration!

      endpoint = ThetaData::Streaming::Endpoint.default

      Sync do
        client = ThetaData::Streaming::Client.open_async(endpoint)

        expect(client).to be_a(ThetaData::Streaming::Client)
        expect(client).not.to be(:closed?)
      ensure
        client&.close
      end
    end

    it "raises error for invalid endpoint within Sync block" do
      endpoint = ThetaData::Streaming::Endpoint.new("nonexistent.invalid.host.example", 20000, timeout: 1)

      Sync do
        expect {
          ThetaData::Streaming::Client.open_async(endpoint)
        }.to raise_exception(SocketError)
      end
    end
  end
end

def skip_unless_integration!
  skip "Set THETADATA_INTEGRATION=1 to run" unless ENV["THETADATA_INTEGRATION"]
end
