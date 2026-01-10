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

  with "#subscribe_full_trades" do
    it "sends trade frame with sec_type payload and returns request_id" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      request_id = client.subscribe_full_trades(ThetaData::Streaming::SecType::STOCK)

      expect(request_id).to be == 1

      stream.rewind
      frame = Protocol::FPSS::Frame.read(stream)

      expect(frame.type).to be == Protocol::FPSS::MessageType::TRADE
      # Payload: [req_id: 4 bytes BE][sec_type: 1 byte]
      expect(frame.payload.bytesize).to be == 5
      req_id, sec_type = frame.payload.unpack("Nc")
      expect(req_id).to be == 1
      expect(sec_type).to be == ThetaData::Streaming::SecType::STOCK
    end

    it "increments request_id shared with per-contract subscriptions" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      contract = ThetaData::Streaming::Contract.stock("AAPL")
      id1 = client.subscribe_trade(contract)
      id2 = client.subscribe_full_trades(ThetaData::Streaming::SecType::STOCK)

      expect(id1).to be == 1
      expect(id2).to be == 2
    end
  end

  with "#subscribe_full_quotes" do
    it "sends quote frame with sec_type payload" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      request_id = client.subscribe_full_quotes(ThetaData::Streaming::SecType::OPTION)

      expect(request_id).to be == 1

      stream.rewind
      frame = Protocol::FPSS::Frame.read(stream)

      expect(frame.type).to be == Protocol::FPSS::MessageType::QUOTE
      req_id, sec_type = frame.payload.unpack("Nc")
      expect(req_id).to be == 1
      expect(sec_type).to be == ThetaData::Streaming::SecType::OPTION
    end
  end

  with "#subscribe_full_open_interest" do
    it "sends open_interest frame with sec_type payload" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      request_id = client.subscribe_full_open_interest(ThetaData::Streaming::SecType::STOCK)

      expect(request_id).to be == 1

      stream.rewind
      frame = Protocol::FPSS::Frame.read(stream)

      expect(frame.type).to be == Protocol::FPSS::MessageType::OPEN_INTEREST
      req_id, sec_type = frame.payload.unpack("Nc")
      expect(req_id).to be == 1
      expect(sec_type).to be == ThetaData::Streaming::SecType::STOCK
    end
  end

  with "firehose sec_type validation" do
    it "raises ArgumentError for index firehose subscription" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      expect { client.subscribe_full_trades(ThetaData::Streaming::SecType::INDEX) }.to raise_exception(ArgumentError)
      expect { client.subscribe_full_quotes(ThetaData::Streaming::SecType::INDEX) }.to raise_exception(ArgumentError)
      expect { client.subscribe_full_open_interest(ThetaData::Streaming::SecType::INDEX) }.to raise_exception(ArgumentError)
    end
  end

  with "#unsubscribe_trade" do
    it "sends REMOVE_TRADE frame with contract payload" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      contract = ThetaData::Streaming::Contract.stock("AAPL")
      sub_id = client.subscribe_trade(contract)
      unsub_id = client.unsubscribe_trade(sub_id)

      stream.rewind
      Protocol::FPSS::Frame.read(stream) # skip subscribe frame
      frame = Protocol::FPSS::Frame.read(stream)

      expect(frame.type).to be == Protocol::FPSS::MessageType::REMOVE_TRADE
      req_id = frame.payload.unpack1("N")
      expect(req_id).to be == unsub_id
    end

    it "removes contract from active_trades" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      contract = ThetaData::Streaming::Contract.stock("AAPL")
      sub_id = client.subscribe_trade(contract)

      active = client.instance_variable_get(:@active_trades)
      expect(active.size).to be == 1

      client.unsubscribe_trade(sub_id)

      expect(active.size).to be == 0
    end

    it "returns nil for unknown subscription id" do
      connection = mock_connection
      client = ThetaData::Streaming::Client.new(connection)

      result = client.unsubscribe_trade(999)
      expect(result).to be == nil
    end
  end

  with "#unsubscribe_quote" do
    it "sends REMOVE_QUOTE frame with contract payload" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      contract = ThetaData::Streaming::Contract.index("SPX")
      sub_id = client.subscribe_quote(contract)
      unsub_id = client.unsubscribe_quote(sub_id)

      stream.rewind
      Protocol::FPSS::Frame.read(stream) # skip subscribe frame
      frame = Protocol::FPSS::Frame.read(stream)

      expect(frame.type).to be == Protocol::FPSS::MessageType::REMOVE_QUOTE
      req_id = frame.payload.unpack1("N")
      expect(req_id).to be == unsub_id
    end

    it "removes contract from active_quotes" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      contract = ThetaData::Streaming::Contract.index("SPX")
      sub_id = client.subscribe_quote(contract)
      client.unsubscribe_quote(sub_id)

      active = client.instance_variable_get(:@active_quotes)
      expect(active.size).to be == 0
    end
  end

  with "#unsubscribe_full_trades" do
    it "sends REMOVE_TRADE frame with sec_type payload" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      sub_id = client.subscribe_full_trades(ThetaData::Streaming::SecType::STOCK)
      unsub_id = client.unsubscribe_full_trades(sub_id)

      stream.rewind
      Protocol::FPSS::Frame.read(stream) # skip subscribe
      frame = Protocol::FPSS::Frame.read(stream)

      expect(frame.type).to be == Protocol::FPSS::MessageType::REMOVE_TRADE
      expect(frame.payload.bytesize).to be == 5
      req_id, sec_type = frame.payload.unpack("Nc")
      expect(req_id).to be == unsub_id
      expect(sec_type).to be == ThetaData::Streaming::SecType::STOCK
    end

    it "removes from active_full_subs" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      sub_id = client.subscribe_full_trades(ThetaData::Streaming::SecType::STOCK)
      client.unsubscribe_full_trades(sub_id)

      active = client.instance_variable_get(:@active_full_subs)
      expect(active.size).to be == 0
    end
  end

  with "#unsubscribe_full_quotes" do
    it "sends REMOVE_QUOTE frame with sec_type payload" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      sub_id = client.subscribe_full_quotes(ThetaData::Streaming::SecType::OPTION)
      unsub_id = client.unsubscribe_full_quotes(sub_id)

      stream.rewind
      Protocol::FPSS::Frame.read(stream)
      frame = Protocol::FPSS::Frame.read(stream)

      expect(frame.type).to be == Protocol::FPSS::MessageType::REMOVE_QUOTE
      req_id, sec_type = frame.payload.unpack("Nc")
      expect(req_id).to be == unsub_id
      expect(sec_type).to be == ThetaData::Streaming::SecType::OPTION
    end
  end

  with "#unsubscribe_full_open_interest" do
    it "sends REMOVE_OPEN_INTEREST frame with sec_type payload" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      sub_id = client.subscribe_full_open_interest(ThetaData::Streaming::SecType::STOCK)
      unsub_id = client.unsubscribe_full_open_interest(sub_id)

      stream.rewind
      Protocol::FPSS::Frame.read(stream)
      frame = Protocol::FPSS::Frame.read(stream)

      expect(frame.type).to be == Protocol::FPSS::MessageType::REMOVE_OPEN_INTEREST
      req_id, sec_type = frame.payload.unpack("Nc")
      expect(req_id).to be == unsub_id
      expect(sec_type).to be == ThetaData::Streaming::SecType::STOCK
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

    it "parses UNSPECIFIED (-1) disconnect correctly" do
      stream = StringIO.new
      # reason_code -1 = UNSPECIFIED = 0xFFFF on wire
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::DISCONNECTED, "\xFF\xFF").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:type]).to be == :disconnected
      expect(event[:reason_code]).to be == -1
      expect(event[:reason]).to be == "UNSPECIFIED"
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
      # contract_id (4 bytes) + contract bytes (SPX index: 06 03 "SPX" 02)
      contract_bytes = [0x06, 0x03].pack("CC") + "SPX" + [0x02].pack("C")
      payload = "\x00\x1B\x7F\x4B" + contract_bytes
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::CONTRACT, payload).write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:type]).to be == :contract
      expect(event[:contract_id]).to be == 1802059
      expect(event[:contract]).to be_a(ThetaData::Streaming::Contract)
      expect(event[:contract].root).to be == "SPX"
      expect(event[:contract].sec_type).to be == ThetaData::Streaming::SecType::INDEX
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

  def fit_encode(values)
    Protocol::FPSS::FIT::Writer.new.write_changes(values)
  end

  with "OHLCVC event parsing" do
    it "parses all 10 fields correctly" do
      stream = StringIO.new
      values = [99999, 34200000, 450000, 451000, 449000, 450500, 10000, 500, 8, 20240115]
      fit_data = fit_encode(values)
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::OHLCVC, fit_data).write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event[:type]).to be == :ohlcvc
      expect(event[:contract_id]).to be == 99999
      expect(event[:ms_of_day]).to be == 34200000
      expect(event[:open]).to be == 450000
      expect(event[:high]).to be == 451000
      expect(event[:low]).to be == 449000
      expect(event[:close]).to be == 450500
      expect(event[:volume]).to be == 10000
      expect(event[:count]).to be == 500
      expect(event[:price_type]).to be == 8
      expect(event[:date]).to be == 20240115
    end

    it "returns a plain hash (not a stateful tick)" do
      stream = StringIO.new
      fit_data = fit_encode([100, 34200000, 100, 100, 100, 100, 0, 0, 8, 20240115])
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::OHLCVC, fit_data).write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event = client.read_event

      expect(event).to be_a(Hash)
      expect(event[:type]).to be == :ohlcvc
    end
  end

  with "state isolation" do
    it "accumulates trade deltas independently per contract_id" do
      stream = StringIO.new
      # Contract A: price=1000
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE,
        fit_encode([100, 34200000, 0, 0, 0, 1000, 10, 8, 20250110])).write(stream)
      # Contract B: price=2000
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE,
        fit_encode([200, 34200000, 0, 0, 0, 2000, 20, 8, 20250110])).write(stream)
      # Contract A delta: price +50
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE,
        fit_encode([100, 1000, 1, 0, 0, 50, 0, 0, 0])).write(stream)
      # Contract B delta: price -100
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE,
        fit_encode([200, 500, 1, 0, 0, -100, 0, 0, 0])).write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      client.read_event  # A initial
      client.read_event  # B initial
      event_a = client.read_event  # A delta
      event_b = client.read_event  # B delta

      expect(event_a[:tick].price).to be == 1050
      expect(event_a[:tick].ms_of_day).to be == 34201000
      expect(event_b[:tick].price).to be == 1900
      expect(event_b[:tick].ms_of_day).to be == 34200500
    end

    it "accumulates quote deltas independently per contract_id" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::QUOTE,
        fit_encode([100, 34200000, 100, 1, 15000, 0, 200, 2, 15100, 0, 8, 20250110])).write(stream)
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::QUOTE,
        fit_encode([200, 34200000, 50, 1, 20000, 0, 50, 2, 20100, 0, 8, 20250110])).write(stream)
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::QUOTE,
        fit_encode([100, 100, 0, 0, 25, 0, 0, 0, -10, 0, 0, 0])).write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      client.read_event  # contract 100 initial
      client.read_event  # contract 200 initial
      event = client.read_event  # contract 100 delta

      expect(event[:quote].bid).to be == 15025
      expect(event[:quote].ask).to be == 15090
    end

    it "returns duped tick (not internal state reference)" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE,
        fit_encode([100, 34200000, 0, 0, 0, 1000, 10, 8, 20250110])).write(stream)
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE,
        fit_encode([100, 1000, 1, 0, 0, 50, 0, 0, 0])).write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      event1 = client.read_event
      event2 = client.read_event

      # event1 should not have been mutated by event2
      expect(event1[:tick].price).to be == 1000
      expect(event2[:tick].price).to be == 1050
    end

    it "keeps trade and quote state independent for same contract_id" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE,
        fit_encode([100, 34200000, 0, 0, 0, 5000, 10, 8, 20250110])).write(stream)
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::QUOTE,
        fit_encode([100, 34200000, 50, 1, 4990, 0, 50, 2, 5010, 0, 8, 20250110])).write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      client = ThetaData::Streaming::Client.new(connection)

      trade_event = client.read_event
      quote_event = client.read_event

      expect(trade_event[:tick].price).to be == 5000
      expect(quote_event[:quote].bid).to be == 4990
      expect(quote_event[:quote].ask).to be == 5010
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
