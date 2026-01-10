require "thetadata"
require "date"

describe ThetaData::REST::Stock do
  def make_mock_session
    ThetaData::REST::Session.new(
      session_id: "test-session-id",
      user: { stocksSubscription: "PRO" },
    )
  end

  def make_mock_connection(response_data)
    session = make_mock_session
    Object.new.tap do |conn|
      conn.define_singleton_method(:session) { session }
      conn.define_singleton_method(:call) do |method, request|
        @last_call = { method: method, request: request }
        response_data
      end
      conn.define_singleton_method(:last_call) { @last_call }
    end
  end

  def with_mock_connection(response_data, &block)
    mock_conn = make_mock_connection(response_data)
    original = ThetaData::REST.instance_variable_get(:@connection)
    ThetaData::REST.instance_variable_set(:@connection, mock_conn)
    block.call(mock_conn)
  ensure
    ThetaData::REST.instance_variable_set(:@connection, original)
  end

  with ".list_symbols" do
    let(:symbols_response) do
      {
        headers: ["symbol"],
        rows: [["AAPL"], ["MSFT"], ["GOOGL"]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(symbols_response) do |conn|
        ThetaData::REST::Stock.list_symbols
        expect(conn.last_call[:method]).to be == :GetStockListSymbols
      end
    end

    it "returns array of symbol strings" do
      with_mock_connection(symbols_response) do |conn|
        result = ThetaData::REST::Stock.list_symbols
        expect(result).to be == ["AAPL", "MSFT", "GOOGL"]
      end
    end
  end

  with ".list_dates" do
    let(:dates_response) do
      {
        headers: ["date"],
        rows: [["2024-12-01"], ["2024-12-02"], ["2024-12-03"]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(dates_response) do |conn|
        ThetaData::REST::Stock.list_dates("AAPL")
        expect(conn.last_call[:method]).to be == :GetStockListDates
      end
    end

    it "returns array of Date objects" do
      with_mock_connection(dates_response) do |conn|
        result = ThetaData::REST::Stock.list_dates("AAPL")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(Date)
        expect(result.first).to be == Date.new(2024, 12, 1)
      end
    end
  end

  with ".snapshot_ohlc" do
    let(:ohlc_response) do
      {
        headers: %w[timestamp symbol open high low close volume count],
        rows: [[Time.new(2024, 12, 2, 16, 0, 0), "AAPL", BigDecimal("150.00"), BigDecimal("155.00"), BigDecimal("149.00"), BigDecimal("154.50"), 1000000, 5000]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(ohlc_response) do |conn|
        ThetaData::REST::Stock.snapshot_ohlc("AAPL")
        expect(conn.last_call[:method]).to be == :GetStockSnapshotOhlc
      end
    end

    it "returns single SnapshotOHLCRow for single symbol" do
      with_mock_connection(ohlc_response) do |conn|
        result = ThetaData::REST::Stock.snapshot_ohlc("AAPL")
        expect(result).to be_a(ThetaData::REST::SnapshotOHLCRow)
        expect(result.symbol).to be == "AAPL"
        expect(result.close).to be == BigDecimal("154.50")
      end
    end

    it "returns array for multiple symbols" do
      multi_response = {
        headers: %w[timestamp symbol open high low close volume count],
        rows: [
          [Time.new(2024, 12, 2), "AAPL", BigDecimal("150.00"), BigDecimal("155.00"), BigDecimal("149.00"), BigDecimal("154.50"), 1000000, 5000],
          [Time.new(2024, 12, 2), "MSFT", BigDecimal("400.00"), BigDecimal("405.00"), BigDecimal("398.00"), BigDecimal("403.00"), 500000, 3000],
        ],
      }
      with_mock_connection(multi_response) do |conn|
        result = ThetaData::REST::Stock.snapshot_ohlc("AAPL", "MSFT")
        expect(result).to be_a(Array)
        expect(result.length).to be == 2
      end
    end
  end

  with ".snapshot_trade" do
    let(:trade_response) do
      {
        headers: %w[timestamp symbol sequence size condition price],
        rows: [[Time.new(2024, 12, 2, 16, 0, 0), "AAPL", 12345, 100, 0, BigDecimal("154.50")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(trade_response) do |conn|
        ThetaData::REST::Stock.snapshot_trade("AAPL")
        expect(conn.last_call[:method]).to be == :GetStockSnapshotTrade
      end
    end

    it "returns SnapshotTradeRow" do
      with_mock_connection(trade_response) do |conn|
        result = ThetaData::REST::Stock.snapshot_trade("AAPL")
        expect(result).to be_a(ThetaData::REST::SnapshotTradeRow)
        expect(result.symbol).to be == "AAPL"
        expect(result.price).to be == BigDecimal("154.50")
      end
    end
  end

  with ".snapshot_quote" do
    let(:quote_response) do
      {
        headers: %w[timestamp symbol bid_size bid_exchange bid bid_condition ask_size ask_exchange ask ask_condition],
        rows: [[Time.new(2024, 12, 2, 16, 0, 0), "AAPL", 100, 1, BigDecimal("154.45"), 0, 200, 1, BigDecimal("154.55"), 0]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(quote_response) do |conn|
        ThetaData::REST::Stock.snapshot_quote("AAPL")
        expect(conn.last_call[:method]).to be == :GetStockSnapshotQuote
      end
    end

    it "returns SnapshotQuoteRow" do
      with_mock_connection(quote_response) do |conn|
        result = ThetaData::REST::Stock.snapshot_quote("AAPL")
        expect(result).to be_a(ThetaData::REST::SnapshotQuoteRow)
        expect(result.symbol).to be == "AAPL"
        expect(result.bid).to be == BigDecimal("154.45")
        expect(result.ask).to be == BigDecimal("154.55")
      end
    end
  end

  with ".history_eod" do
    let(:eod_response) do
      {
        headers: %w[created last_trade open high low close volume count bid_size bid_exchange bid bid_condition ask_size ask_exchange ask ask_condition],
        rows: [[Time.new(2024, 12, 1), Time.new(2024, 12, 1), BigDecimal("150.00"), BigDecimal("155.00"), BigDecimal("149.00"), BigDecimal("154.50"), 1000000, 5000, 100, 1, BigDecimal("154.45"), 0, 200, 1, BigDecimal("154.55"), 0]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(eod_response) do |conn|
        ThetaData::REST::Stock.history_eod("AAPL", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31))
        expect(conn.last_call[:method]).to be == :GetStockHistoryEod
      end
    end

    it "returns EODRow Data objects" do
      with_mock_connection(eod_response) do |conn|
        result = ThetaData::REST::Stock.history_eod("AAPL", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::EODRow)
        expect(result.first.close).to be == BigDecimal("154.50")
      end
    end
  end

  with ".history_ohlc" do
    let(:ohlc_response) do
      {
        headers: %w[timestamp open high low close volume count vwap],
        rows: [[Time.new(2024, 12, 1, 9, 30, 0), BigDecimal("150.00"), BigDecimal("151.00"), BigDecimal("149.50"), BigDecimal("150.75"), 10000, 50, BigDecimal("150.50")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(ohlc_response) do |conn|
        ThetaData::REST::Stock.history_ohlc("AAPL", date: Date.new(2024, 12, 1), interval: 60000)
        expect(conn.last_call[:method]).to be == :GetStockHistoryOhlc
      end
    end

    it "returns OHLCRow Data objects" do
      with_mock_connection(ohlc_response) do |conn|
        result = ThetaData::REST::Stock.history_ohlc("AAPL", date: Date.new(2024, 12, 1), interval: 60000)
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::OHLCRow)
        expect(result.first.close).to be == BigDecimal("150.75")
        expect(result.first.vwap).to be == BigDecimal("150.50")
      end
    end
  end

  with ".history_trade" do
    let(:trade_response) do
      {
        headers: %w[timestamp sequence ext_condition1 ext_condition2 ext_condition3 ext_condition4 condition size exchange price],
        rows: [[Time.new(2024, 12, 1, 9, 30, 0), 12345, 0, 0, 0, 0, 0, 100, 1, BigDecimal("150.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(trade_response) do |conn|
        ThetaData::REST::Stock.history_trade("AAPL", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetStockHistoryTrade
      end
    end

    it "returns TradeRow Data objects" do
      with_mock_connection(trade_response) do |conn|
        result = ThetaData::REST::Stock.history_trade("AAPL", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::TradeRow)
        expect(result.first.price).to be == BigDecimal("150.00")
      end
    end
  end

  with ".history_quote" do
    let(:quote_response) do
      {
        headers: %w[timestamp bid_size bid_exchange bid bid_condition ask_size ask_exchange ask ask_condition],
        rows: [[Time.new(2024, 12, 1, 9, 30, 0), 100, 1, BigDecimal("149.95"), 0, 200, 1, BigDecimal("150.05"), 0]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(quote_response) do |conn|
        ThetaData::REST::Stock.history_quote("AAPL", date: Date.new(2024, 12, 1), interval: 60000)
        expect(conn.last_call[:method]).to be == :GetStockHistoryQuote
      end
    end

    it "returns QuoteRow Data objects" do
      with_mock_connection(quote_response) do |conn|
        result = ThetaData::REST::Stock.history_quote("AAPL", date: Date.new(2024, 12, 1), interval: 60000)
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::QuoteRow)
        expect(result.first.bid).to be == BigDecimal("149.95")
      end
    end
  end

  with ".history_trade_quote" do
    let(:trade_quote_response) do
      {
        headers: %w[trade_timestamp quote_timestamp sequence ext_condition1 ext_condition2 ext_condition3 ext_condition4 condition size exchange price bid_size bid_exchange bid bid_condition ask_size ask_exchange ask ask_condition],
        rows: [[Time.new(2024, 12, 1, 9, 30, 0), Time.new(2024, 12, 1, 9, 30, 0), 12345, 0, 0, 0, 0, 0, 100, 1, BigDecimal("150.00"), 100, 1, BigDecimal("149.95"), 0, 200, 1, BigDecimal("150.05"), 0]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(trade_quote_response) do |conn|
        ThetaData::REST::Stock.history_trade_quote("AAPL", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetStockHistoryTradeQuote
      end
    end

    it "returns TradeQuoteRow Data objects" do
      with_mock_connection(trade_quote_response) do |conn|
        result = ThetaData::REST::Stock.history_trade_quote("AAPL", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::TradeQuoteRow)
        expect(result.first.price).to be == BigDecimal("150.00")
        expect(result.first.bid).to be == BigDecimal("149.95")
      end
    end
  end

  with ".at_time_trade" do
    let(:at_time_response) do
      {
        headers: %w[timestamp sequence ext_condition1 ext_condition2 ext_condition3 ext_condition4 condition size exchange price],
        rows: [[Time.new(2024, 12, 1, 10, 0, 0), 12345, 0, 0, 0, 0, 0, 100, 1, BigDecimal("150.50")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(at_time_response) do |conn|
        ThetaData::REST::Stock.at_time_trade("AAPL", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31), time_of_day: "10:00:00")
        expect(conn.last_call[:method]).to be == :GetStockAtTimeTrade
      end
    end

    it "returns TradeRow Data objects" do
      with_mock_connection(at_time_response) do |conn|
        result = ThetaData::REST::Stock.at_time_trade("AAPL", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31), time_of_day: "10:00:00")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::TradeRow)
      end
    end
  end

  with ".at_time_quote" do
    let(:at_time_response) do
      {
        headers: %w[timestamp bid_size bid_exchange bid bid_condition ask_size ask_exchange ask ask_condition],
        rows: [[Time.new(2024, 12, 1, 10, 0, 0), 100, 1, BigDecimal("150.45"), 0, 200, 1, BigDecimal("150.55"), 0]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(at_time_response) do |conn|
        ThetaData::REST::Stock.at_time_quote("AAPL", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31), time_of_day: "10:00:00")
        expect(conn.last_call[:method]).to be == :GetStockAtTimeQuote
      end
    end

    it "returns QuoteRow Data objects" do
      with_mock_connection(at_time_response) do |conn|
        result = ThetaData::REST::Stock.at_time_quote("AAPL", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31), time_of_day: "10:00:00")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::QuoteRow)
      end
    end
  end
end
