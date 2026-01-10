require "thetadata"
require "stringio"

describe "Streaming end-to-end" do
  let(:writer) { Protocol::FPSS::FIT::Writer.new }

  def build_stream
    StringIO.new.tap {|s| s.set_encoding("ASCII-8BIT") }
  end

  def write_trade(stream, values)
    data = writer.write_changes(values)
    Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE, data).write(stream)
  end

  def write_quote(stream, values)
    data = writer.write_changes(values)
    Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::QUOTE, data).write(stream)
  end

  def write_ohlcvc(stream, values)
    data = writer.write_changes(values)
    Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::OHLCVC, data).write(stream)
  end

  def client_from(stream)
    stream.rewind
    connection = ThetaData::Streaming::Connection.new(stream)
    ThetaData::Streaming::Client.new(connection)
  end

  with "single trade tick" do
    it "round-trips from FIT encoding through Client to TradeTick" do
      values = [1802875, 36340000, 0, 0, 0, 553020, 5, 8, 20250428]

      stream = build_stream
      write_trade(stream, values)
      client = client_from(stream)

      event = client.read_event

      expect(event[:type]).to be == :trade
      expect(event[:contract_id]).to be == 1802875

      tick = event[:tick]
      expect(tick.contract_id).to be == 1802875
      expect(tick.ms_of_day).to be == 36340000
      expect(tick.sequence).to be == 0
      expect(tick.ext_con1).to be == 0
      expect(tick.ext_con2).to be == 0
      expect(tick.price).to be == 553020
      expect(tick.size).to be == 5
      expect(tick.price_type).to be == 8
      expect(tick.date).to be == 20250428
      expect(tick.price_decimal).to be == BigDecimal("5530.20")
      expect(tick.time).to be == "10:05:40.000"
    end
  end

  with "trade tick delta sequence" do
    it "accumulates deltas correctly across 3 messages" do
      stream = build_stream
      write_trade(stream, [1802875, 36340000, 0, 0, 0, 553020, 5, 8, 20250428])
      write_trade(stream, [1802875, 1000, 1, 0, 0, -8, 3, 0, 0])
      write_trade(stream, [1802875, 500, 1, 0, 0, 15, 1, 0, 0])
      client = client_from(stream)

      e1 = client.read_event
      expect(e1[:tick].price).to be == 553020
      expect(e1[:tick].price_decimal).to be == BigDecimal("5530.20")

      e2 = client.read_event
      expect(e2[:tick].price).to be == 553012
      expect(e2[:tick].ms_of_day).to be == 36341000
      expect(e2[:tick].sequence).to be == 1
      expect(e2[:tick].size).to be == 8
      expect(e2[:tick].price_decimal).to be == BigDecimal("5530.12")

      e3 = client.read_event
      expect(e3[:tick].price).to be == 553027
      expect(e3[:tick].ms_of_day).to be == 36341500
      expect(e3[:tick].sequence).to be == 2
      expect(e3[:tick].size).to be == 9
      expect(e3[:tick].price_decimal).to be == BigDecimal("5530.27")
    end
  end

  with "single quote tick" do
    it "round-trips from FIT encoding through Client to QuoteTick" do
      values = [1802875, 53790000, 100, 1, 15000, 0, 200, 2, 15100, 0, 8, 20250110]

      stream = build_stream
      write_quote(stream, values)
      client = client_from(stream)

      event = client.read_event

      expect(event[:type]).to be == :quote
      quote = event[:quote]
      expect(quote.contract_id).to be == 1802875
      expect(quote.bid).to be == 15000
      expect(quote.ask).to be == 15100
      expect(quote.bid_size).to be == 100
      expect(quote.ask_size).to be == 200
      expect(quote.bid_decimal).to be == BigDecimal("150.00")
      expect(quote.ask_decimal).to be == BigDecimal("151.00")
      expect(quote.mid_decimal).to be == BigDecimal("150.50")
      expect(quote.spread_decimal).to be == BigDecimal("1.00")
    end
  end

  with "OHLCVC" do
    it "round-trips from FIT encoding through Client to event hash" do
      values = [99999, 34200000, 450000, 451000, 449000, 450500, 10000, 500, 8, 20240115]

      stream = build_stream
      write_ohlcvc(stream, values)
      client = client_from(stream)

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
  end

  with "mixed message stream" do
    it "parses mixed event types in order" do
      stream = build_stream
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::METADATA, "STOCK.FREE, INDEX.PRO").write(stream)

      payload = [1, 0].pack("N2")
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::REQ_RESPONSE, payload).write(stream)

      contract_payload = [1802059].pack("N") + "info"
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::CONTRACT, contract_payload).write(stream)

      write_trade(stream, [1802875, 36340000, 0, 0, 0, 553020, 5, 8, 20250428])
      write_quote(stream, [1802875, 53790000, 100, 1, 15000, 0, 200, 2, 15100, 0, 8, 20250110])

      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(stream)

      client = client_from(stream)

      types = []
      client.each_event {|e| types << e[:type] }

      expect(types).to be == [:metadata, :req_response, :contract, :trade, :quote, :ping]
    end
  end

  with "two interleaved contracts" do
    it "maintains independent state per contract" do
      stream = build_stream
      # Contract A (SPX): initial
      write_trade(stream, [100, 34200000, 0, 0, 0, 553020, 5, 8, 20250428])
      # Contract B (VIX): initial
      write_trade(stream, [200, 34200000, 0, 0, 0, 2100, 10, 8, 20250428])
      # Contract A delta: price +80
      write_trade(stream, [100, 1000, 1, 0, 0, 80, 0, 0, 0])
      # Contract B delta: price -50
      write_trade(stream, [200, 500, 1, 0, 0, -50, 0, 0, 0])

      client = client_from(stream)

      a_init = client.read_event
      b_init = client.read_event
      a_delta = client.read_event
      b_delta = client.read_event

      expect(a_init[:tick].price).to be == 553020
      expect(b_init[:tick].price).to be == 2100

      expect(a_delta[:tick].price).to be == 553100
      expect(a_delta[:tick].ms_of_day).to be == 34201000
      expect(a_delta[:tick].price_decimal).to be == BigDecimal("5531.00")

      expect(b_delta[:tick].price).to be == 2050
      expect(b_delta[:tick].ms_of_day).to be == 34200500
      expect(b_delta[:tick].price_decimal).to be == BigDecimal("20.50")
    end
  end

  with "quote delta sequence" do
    it "accumulates quote deltas correctly" do
      stream = build_stream
      write_quote(stream, [1802875, 53790000, 100, 1, 15000, 0, 200, 2, 15100, 0, 8, 20250110])
      write_quote(stream, [1802875, 100, 50, 0, 25, 0, 0, 0, -50, 0, 0, 0])
      client = client_from(stream)

      client.read_event  # initial
      event = client.read_event  # delta

      quote = event[:quote]
      expect(quote.bid).to be == 15025
      expect(quote.bid_size).to be == 150
      expect(quote.ask).to be == 15050
      expect(quote.ask_size).to be == 200
      expect(quote.bid_decimal).to be == BigDecimal("150.25")
      expect(quote.ask_decimal).to be == BigDecimal("150.50")
      expect(quote.spread_decimal).to be == BigDecimal("0.25")
    end
  end
end
