require "thetadata"
require "date"

describe ThetaData::REST::Option do
  def make_mock_session
    ThetaData::REST::Session.new(
      session_id: "test-session-id",
      user: { optionsSubscription: "PRO" },
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
        rows: [["AAPL"], ["MSFT"], ["SPY"]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(symbols_response) do |conn|
        ThetaData::REST::Option.list_symbols
        expect(conn.last_call[:method]).to be == :GetOptionListSymbols
      end
    end

    it "returns array of symbol strings" do
      with_mock_connection(symbols_response) do |conn|
        result = ThetaData::REST::Option.list_symbols
        expect(result).to be == ["AAPL", "MSFT", "SPY"]
      end
    end
  end

  with ".list_expirations" do
    let(:expirations_response) do
      {
        headers: %w[symbol expiration],
        rows: [["AAPL", "2025-01-17"], ["AAPL", "2025-02-21"], ["AAPL", "2025-03-21"]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(expirations_response) do |conn|
        ThetaData::REST::Option.list_expirations("AAPL")
        expect(conn.last_call[:method]).to be == :GetOptionListExpirations
      end
    end

    it "returns ExpirationRow Data objects" do
      with_mock_connection(expirations_response) do |conn|
        result = ThetaData::REST::Option.list_expirations("AAPL")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::ExpirationRow)
        expect(result.first.symbol).to be == "AAPL"
        expect(result.first.expiration).to be == "2025-01-17"
      end
    end
  end

  with ".list_strikes" do
    let(:strikes_response) do
      {
        headers: %w[symbol strike],
        rows: [["AAPL", BigDecimal("150.00")], ["AAPL", BigDecimal("155.00")], ["AAPL", BigDecimal("160.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(strikes_response) do |conn|
        ThetaData::REST::Option.list_strikes("AAPL", expiration: "2025-01-17")
        expect(conn.last_call[:method]).to be == :GetOptionListStrikes
      end
    end

    it "returns StrikeRow Data objects" do
      with_mock_connection(strikes_response) do |conn|
        result = ThetaData::REST::Option.list_strikes("AAPL", expiration: "2025-01-17")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::StrikeRow)
        expect(result.first.strike).to be == BigDecimal("150.00")
      end
    end
  end

  with ".list_contracts" do
    let(:contracts_response) do
      {
        headers: %w[symbol expiration strike right],
        rows: [
          ["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL"],
          ["AAPL", "2025-01-17", BigDecimal("150.00"), "PUT"],
        ],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(contracts_response) do |conn|
        ThetaData::REST::Option.list_contracts("AAPL", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetOptionListContracts
      end
    end

    it "returns ContractRow Data objects" do
      with_mock_connection(contracts_response) do |conn|
        result = ThetaData::REST::Option.list_contracts("AAPL", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::ContractRow)
        expect(result.first.right).to be == "CALL"
      end
    end
  end

  with ".snapshot_ohlc" do
    let(:ohlc_response) do
      {
        headers: %w[timestamp symbol expiration strike right open high low close volume count],
        rows: [[Time.new(2024, 12, 2), "AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", BigDecimal("5.00"), BigDecimal("5.50"), BigDecimal("4.80"), BigDecimal("5.25"), 1000, 50]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(ohlc_response) do |conn|
        ThetaData::REST::Option.snapshot_ohlc(symbol: "AAPL", expiration: "2025-01-17", strike: 150, right: "CALL")
        expect(conn.last_call[:method]).to be == :GetOptionSnapshotOhlc
      end
    end

    it "returns OptionSnapshotOHLCRow Data objects" do
      with_mock_connection(ohlc_response) do |conn|
        result = ThetaData::REST::Option.snapshot_ohlc(symbol: "AAPL", expiration: "2025-01-17")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::OptionSnapshotOHLCRow)
        expect(result.first.close).to be == BigDecimal("5.25")
      end
    end
  end

  with ".snapshot_trade" do
    let(:trade_response) do
      {
        headers: %w[symbol expiration strike right timestamp sequence ext_condition1 ext_condition2 ext_condition3 ext_condition4 condition size exchange price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 2), 12345, 0, 0, 0, 0, 0, 10, 1, BigDecimal("5.25")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(trade_response) do |conn|
        ThetaData::REST::Option.snapshot_trade(symbol: "AAPL", expiration: "2025-01-17", strike: 150, right: "CALL")
        expect(conn.last_call[:method]).to be == :GetOptionSnapshotTrade
      end
    end

    it "returns OptionSnapshotTradeRow Data objects" do
      with_mock_connection(trade_response) do |conn|
        result = ThetaData::REST::Option.snapshot_trade(symbol: "AAPL", expiration: "2025-01-17")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::OptionSnapshotTradeRow)
        expect(result.first.price).to be == BigDecimal("5.25")
      end
    end
  end

  with ".snapshot_quote" do
    let(:quote_response) do
      {
        headers: %w[timestamp symbol expiration strike right bid_size bid_exchange bid bid_condition ask_size ask_exchange ask ask_condition],
        rows: [[Time.new(2024, 12, 2), "AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", 100, 1, BigDecimal("5.20"), 0, 200, 1, BigDecimal("5.30"), 0]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(quote_response) do |conn|
        ThetaData::REST::Option.snapshot_quote(symbol: "AAPL", expiration: "2025-01-17", strike: 150, right: "CALL")
        expect(conn.last_call[:method]).to be == :GetOptionSnapshotQuote
      end
    end

    it "returns OptionSnapshotQuoteRow Data objects" do
      with_mock_connection(quote_response) do |conn|
        result = ThetaData::REST::Option.snapshot_quote(symbol: "AAPL", expiration: "2025-01-17")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::OptionSnapshotQuoteRow)
        expect(result.first.bid).to be == BigDecimal("5.20")
        expect(result.first.ask).to be == BigDecimal("5.30")
      end
    end
  end

  with ".snapshot_open_interest" do
    let(:oi_response) do
      {
        headers: %w[timestamp symbol expiration strike right open_interest],
        rows: [[Time.new(2024, 12, 2), "AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", 5000]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(oi_response) do |conn|
        ThetaData::REST::Option.snapshot_open_interest(symbol: "AAPL", expiration: "2025-01-17", strike: 150, right: "CALL")
        expect(conn.last_call[:method]).to be == :GetOptionSnapshotOpenInterest
      end
    end

    it "returns SnapshotOpenInterestRow Data objects" do
      with_mock_connection(oi_response) do |conn|
        result = ThetaData::REST::Option.snapshot_open_interest(symbol: "AAPL", expiration: "2025-01-17")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::SnapshotOpenInterestRow)
        expect(result.first.open_interest).to be == 5000
      end
    end
  end

  with ".history_eod" do
    let(:eod_response) do
      {
        headers: %w[symbol expiration strike right created last_trade open high low close volume count bid_size bid_exchange bid bid_condition ask_size ask_exchange ask ask_condition],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1), Time.new(2024, 12, 1), BigDecimal("5.00"), BigDecimal("5.50"), BigDecimal("4.80"), BigDecimal("5.25"), 1000, 50, 100, 1, BigDecimal("5.20"), 0, 200, 1, BigDecimal("5.30"), 0]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(eod_response) do |conn|
        ThetaData::REST::Option.history_eod(symbol: "AAPL", expiration: "2025-01-17", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryEod
      end
    end

    it "returns OptionEODRow Data objects" do
      with_mock_connection(eod_response) do |conn|
        result = ThetaData::REST::Option.history_eod(symbol: "AAPL", expiration: "2025-01-17", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::OptionEODRow)
        expect(result.first.close).to be == BigDecimal("5.25")
      end
    end
  end

  with ".history_ohlc" do
    let(:ohlc_response) do
      {
        headers: %w[symbol expiration strike right timestamp open high low close volume count vwap],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 9, 30, 0), BigDecimal("5.00"), BigDecimal("5.10"), BigDecimal("4.95"), BigDecimal("5.05"), 100, 10, BigDecimal("5.02")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(ohlc_response) do |conn|
        ThetaData::REST::Option.history_ohlc(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1), interval: 60000)
        expect(conn.last_call[:method]).to be == :GetOptionHistoryOhlc
      end
    end

    it "returns OptionOHLCRow Data objects" do
      with_mock_connection(ohlc_response) do |conn|
        result = ThetaData::REST::Option.history_ohlc(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1), interval: 60000)
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::OptionOHLCRow)
        expect(result.first.vwap).to be == BigDecimal("5.02")
      end
    end
  end

  with ".history_trade" do
    let(:trade_response) do
      {
        headers: %w[symbol expiration strike right timestamp sequence ext_condition1 ext_condition2 ext_condition3 ext_condition4 condition size exchange price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 9, 30, 0), 12345, 0, 0, 0, 0, 0, 10, 1, BigDecimal("5.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(trade_response) do |conn|
        ThetaData::REST::Option.history_trade(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryTrade
      end
    end

    it "returns OptionTradeRow Data objects" do
      with_mock_connection(trade_response) do |conn|
        result = ThetaData::REST::Option.history_trade(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::OptionTradeRow)
        expect(result.first.price).to be == BigDecimal("5.00")
      end
    end
  end

  with ".history_quote" do
    let(:quote_response) do
      {
        headers: %w[symbol expiration strike right timestamp bid_size bid_exchange bid bid_condition ask_size ask_exchange ask ask_condition],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 9, 30, 0), 100, 1, BigDecimal("4.95"), 0, 200, 1, BigDecimal("5.05"), 0]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(quote_response) do |conn|
        ThetaData::REST::Option.history_quote(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1), interval: 60000)
        expect(conn.last_call[:method]).to be == :GetOptionHistoryQuote
      end
    end

    it "returns OptionQuoteRow Data objects" do
      with_mock_connection(quote_response) do |conn|
        result = ThetaData::REST::Option.history_quote(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1), interval: 60000)
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::OptionQuoteRow)
        expect(result.first.bid).to be == BigDecimal("4.95")
      end
    end
  end

  with ".history_trade_quote" do
    let(:trade_quote_response) do
      {
        headers: %w[symbol expiration strike right trade_timestamp quote_timestamp sequence ext_condition1 ext_condition2 ext_condition3 ext_condition4 condition size exchange price bid_size bid_exchange bid bid_condition ask_size ask_exchange ask ask_condition],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 9, 30, 0), Time.new(2024, 12, 1, 9, 30, 0), 12345, 0, 0, 0, 0, 0, 10, 1, BigDecimal("5.00"), 100, 1, BigDecimal("4.95"), 0, 200, 1, BigDecimal("5.05"), 0]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(trade_quote_response) do |conn|
        ThetaData::REST::Option.history_trade_quote(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryTradeQuote
      end
    end

    it "returns OptionTradeQuoteRow Data objects" do
      with_mock_connection(trade_quote_response) do |conn|
        result = ThetaData::REST::Option.history_trade_quote(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::OptionTradeQuoteRow)
        expect(result.first.price).to be == BigDecimal("5.00")
        expect(result.first.bid).to be == BigDecimal("4.95")
      end
    end
  end

  with ".history_open_interest" do
    let(:oi_response) do
      {
        headers: %w[symbol expiration strike right timestamp open_interest],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1), 5000]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(oi_response) do |conn|
        ThetaData::REST::Option.history_open_interest(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryOpenInterest
      end
    end

    it "returns OptionOpenInterestRow Data objects" do
      with_mock_connection(oi_response) do |conn|
        result = ThetaData::REST::Option.history_open_interest(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::OptionOpenInterestRow)
        expect(result.first.open_interest).to be == 5000
      end
    end
  end

  with ".at_time_trade" do
    let(:at_time_response) do
      {
        headers: %w[symbol expiration strike right timestamp sequence ext_condition1 ext_condition2 ext_condition3 ext_condition4 condition size exchange price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 10, 0, 0), 12345, 0, 0, 0, 0, 0, 10, 1, BigDecimal("5.10")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(at_time_response) do |conn|
        ThetaData::REST::Option.at_time_trade(symbol: "AAPL", expiration: "2025-01-17", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31), time_of_day: "10:00:00")
        expect(conn.last_call[:method]).to be == :GetOptionAtTimeTrade
      end
    end

    it "returns OptionTradeRow Data objects" do
      with_mock_connection(at_time_response) do |conn|
        result = ThetaData::REST::Option.at_time_trade(symbol: "AAPL", expiration: "2025-01-17", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31), time_of_day: "10:00:00")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::OptionTradeRow)
      end
    end
  end

  with ".at_time_quote" do
    let(:at_time_response) do
      {
        headers: %w[symbol expiration strike right timestamp bid_size bid_exchange bid bid_condition ask_size ask_exchange ask ask_condition],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 10, 0, 0), 100, 1, BigDecimal("5.05"), 0, 200, 1, BigDecimal("5.15"), 0]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(at_time_response) do |conn|
        ThetaData::REST::Option.at_time_quote(symbol: "AAPL", expiration: "2025-01-17", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31), time_of_day: "10:00:00")
        expect(conn.last_call[:method]).to be == :GetOptionAtTimeQuote
      end
    end

    it "returns OptionQuoteRow Data objects" do
      with_mock_connection(at_time_response) do |conn|
        result = ThetaData::REST::Option.at_time_quote(symbol: "AAPL", expiration: "2025-01-17", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31), time_of_day: "10:00:00")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::OptionQuoteRow)
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
        ThetaData::REST::Option.list_dates(symbol: "AAPL", expiration: "2025-01-17")
        expect(conn.last_call[:method]).to be == :GetOptionListDates
      end
    end

    it "returns array of Date objects" do
      with_mock_connection(dates_response) do |conn|
        result = ThetaData::REST::Option.list_dates(symbol: "AAPL", expiration: "2025-01-17")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(Date)
        expect(result.first).to be == Date.new(2024, 12, 1)
      end
    end
  end

  with ".snapshot_greeks_implied_volatility" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp bid ask implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 2), BigDecimal("5.20"), BigDecimal("5.30"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 2), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.snapshot_greeks_implied_volatility(symbol: "AAPL", expiration: "2025-01-17")
        expect(conn.last_call[:method]).to be == :GetOptionSnapshotGreeksImpliedVolatility
      end
    end

    it "returns GreeksImpliedVolatilityRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.snapshot_greeks_implied_volatility(symbol: "AAPL", expiration: "2025-01-17")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::GreeksImpliedVolatilityRow)
        expect(result.first.implied_vol).to be == BigDecimal("0.25")
      end
    end
  end

  with ".snapshot_greeks_all" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp bid ask delta theta vega rho epsilon lambda gamma vanna charm vomma veta vera speed zomma color ultima d1 d2 dual_delta dual_gamma implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 2), BigDecimal("5.20"), BigDecimal("5.30"), BigDecimal("0.55"), BigDecimal("-0.05"), BigDecimal("0.20"), BigDecimal("0.10"), BigDecimal("-0.08"), BigDecimal("15.5"), BigDecimal("0.02"), BigDecimal("0.01"), BigDecimal("-0.005"), BigDecimal("0.5"), BigDecimal("0.1"), BigDecimal("0.0"), BigDecimal("0.001"), BigDecimal("0.0002"), BigDecimal("-0.0001"), BigDecimal("0.5"), BigDecimal("0.4"), BigDecimal("0.3"), BigDecimal("-0.45"), BigDecimal("0.02"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 2), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.snapshot_greeks_all(symbol: "AAPL", expiration: "2025-01-17")
        expect(conn.last_call[:method]).to be == :GetOptionSnapshotGreeksAll
      end
    end

    it "returns GreeksAllRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.snapshot_greeks_all(symbol: "AAPL", expiration: "2025-01-17")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::GreeksAllRow)
        expect(result.first.delta).to be == BigDecimal("0.55")
      end
    end
  end

  with ".snapshot_greeks_first_order" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp bid ask delta theta vega rho epsilon lambda implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 2), BigDecimal("5.20"), BigDecimal("5.30"), BigDecimal("0.55"), BigDecimal("-0.05"), BigDecimal("0.20"), BigDecimal("0.10"), BigDecimal("-0.08"), BigDecimal("15.5"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 2), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.snapshot_greeks_first_order(symbol: "AAPL", expiration: "2025-01-17")
        expect(conn.last_call[:method]).to be == :GetOptionSnapshotGreeksFirstOrder
      end
    end

    it "returns GreeksFirstOrderRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.snapshot_greeks_first_order(symbol: "AAPL", expiration: "2025-01-17")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::GreeksFirstOrderRow)
        expect(result.first.delta).to be == BigDecimal("0.55")
        expect(result.first.theta).to be == BigDecimal("-0.05")
      end
    end
  end

  with ".snapshot_greeks_second_order" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp bid ask gamma vanna charm vomma veta implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 2), BigDecimal("5.20"), BigDecimal("5.30"), BigDecimal("0.02"), BigDecimal("0.01"), BigDecimal("-0.005"), BigDecimal("0.5"), BigDecimal("0.1"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 2), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.snapshot_greeks_second_order(symbol: "AAPL", expiration: "2025-01-17")
        expect(conn.last_call[:method]).to be == :GetOptionSnapshotGreeksSecondOrder
      end
    end

    it "returns GreeksSecondOrderRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.snapshot_greeks_second_order(symbol: "AAPL", expiration: "2025-01-17")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::GreeksSecondOrderRow)
        expect(result.first.gamma).to be == BigDecimal("0.02")
      end
    end
  end

  with ".snapshot_greeks_third_order" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp bid ask speed zomma color ultima implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 2), BigDecimal("5.20"), BigDecimal("5.30"), BigDecimal("0.001"), BigDecimal("0.0002"), BigDecimal("-0.0001"), BigDecimal("0.5"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 2), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.snapshot_greeks_third_order(symbol: "AAPL", expiration: "2025-01-17")
        expect(conn.last_call[:method]).to be == :GetOptionSnapshotGreeksThirdOrder
      end
    end

    it "returns GreeksThirdOrderRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.snapshot_greeks_third_order(symbol: "AAPL", expiration: "2025-01-17")
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::GreeksThirdOrderRow)
        expect(result.first.speed).to be == BigDecimal("0.001")
      end
    end
  end

  with ".history_greeks_eod" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp open high low close volume count bid_size bid_exchange bid bid_condition ask_size ask_exchange ask ask_condition delta theta vega rho epsilon lambda gamma vanna charm vomma veta vera speed zomma color ultima d1 d2 dual_delta dual_gamma implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1), BigDecimal("5.00"), BigDecimal("5.50"), BigDecimal("4.80"), BigDecimal("5.25"), 1000, 50, 100, 1, BigDecimal("5.20"), 0, 200, 1, BigDecimal("5.30"), 0, BigDecimal("0.55"), BigDecimal("-0.05"), BigDecimal("0.20"), BigDecimal("0.10"), BigDecimal("-0.08"), BigDecimal("15.5"), BigDecimal("0.02"), BigDecimal("0.01"), BigDecimal("-0.005"), BigDecimal("0.5"), BigDecimal("0.1"), BigDecimal("0.0"), BigDecimal("0.001"), BigDecimal("0.0002"), BigDecimal("-0.0001"), BigDecimal("0.5"), BigDecimal("0.4"), BigDecimal("0.3"), BigDecimal("-0.45"), BigDecimal("0.02"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 1), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.history_greeks_eod(symbol: "AAPL", expiration: "2025-01-17", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryGreeksEod
      end
    end

    it "returns GreeksEODRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.history_greeks_eod(symbol: "AAPL", expiration: "2025-01-17", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::GreeksEODRow)
        expect(result.first.delta).to be == BigDecimal("0.55")
        expect(result.first.close).to be == BigDecimal("5.25")
      end
    end
  end

  with ".history_greeks_all" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp bid ask delta theta vega rho epsilon lambda gamma vanna charm vomma veta vera speed zomma color ultima d1 d2 dual_delta dual_gamma implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 9, 30, 0), BigDecimal("5.20"), BigDecimal("5.30"), BigDecimal("0.55"), BigDecimal("-0.05"), BigDecimal("0.20"), BigDecimal("0.10"), BigDecimal("-0.08"), BigDecimal("15.5"), BigDecimal("0.02"), BigDecimal("0.01"), BigDecimal("-0.005"), BigDecimal("0.5"), BigDecimal("0.1"), BigDecimal("0.0"), BigDecimal("0.001"), BigDecimal("0.0002"), BigDecimal("-0.0001"), BigDecimal("0.5"), BigDecimal("0.4"), BigDecimal("0.3"), BigDecimal("-0.45"), BigDecimal("0.02"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 1), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.history_greeks_all(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryGreeksAll
      end
    end

    it "returns GreeksAllRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.history_greeks_all(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::GreeksAllRow)
        expect(result.first.delta).to be == BigDecimal("0.55")
      end
    end
  end

  with ".history_greeks_first_order" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp bid ask delta theta vega rho epsilon lambda implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 9, 30, 0), BigDecimal("5.20"), BigDecimal("5.30"), BigDecimal("0.55"), BigDecimal("-0.05"), BigDecimal("0.20"), BigDecimal("0.10"), BigDecimal("-0.08"), BigDecimal("15.5"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 1), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.history_greeks_first_order(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryGreeksFirstOrder
      end
    end

    it "returns GreeksFirstOrderRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.history_greeks_first_order(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::GreeksFirstOrderRow)
      end
    end
  end

  with ".history_greeks_second_order" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp bid ask gamma vanna charm vomma veta implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 9, 30, 0), BigDecimal("5.20"), BigDecimal("5.30"), BigDecimal("0.02"), BigDecimal("0.01"), BigDecimal("-0.005"), BigDecimal("0.5"), BigDecimal("0.1"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 1), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.history_greeks_second_order(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryGreeksSecondOrder
      end
    end

    it "returns GreeksSecondOrderRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.history_greeks_second_order(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::GreeksSecondOrderRow)
      end
    end
  end

  with ".history_greeks_third_order" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp bid ask speed zomma color ultima implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 9, 30, 0), BigDecimal("5.20"), BigDecimal("5.30"), BigDecimal("0.001"), BigDecimal("0.0002"), BigDecimal("-0.0001"), BigDecimal("0.5"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 1), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.history_greeks_third_order(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryGreeksThirdOrder
      end
    end

    it "returns GreeksThirdOrderRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.history_greeks_third_order(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::GreeksThirdOrderRow)
      end
    end
  end

  with ".history_greeks_implied_volatility" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp bid ask implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 9, 30, 0), BigDecimal("5.20"), BigDecimal("5.30"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 1), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.history_greeks_implied_volatility(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryGreeksImpliedVolatility
      end
    end

    it "returns GreeksImpliedVolatilityRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.history_greeks_implied_volatility(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::GreeksImpliedVolatilityRow)
      end
    end
  end

  with ".history_trade_greeks_all" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp sequence ext_condition1 ext_condition2 ext_condition3 ext_condition4 condition size exchange price delta theta vega rho epsilon lambda gamma vanna charm vomma veta vera speed zomma color ultima d1 d2 dual_delta dual_gamma implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 9, 30, 0), 12345, 0, 0, 0, 0, 0, 10, 1, BigDecimal("5.25"), BigDecimal("0.55"), BigDecimal("-0.05"), BigDecimal("0.20"), BigDecimal("0.10"), BigDecimal("-0.08"), BigDecimal("15.5"), BigDecimal("0.02"), BigDecimal("0.01"), BigDecimal("-0.005"), BigDecimal("0.5"), BigDecimal("0.1"), BigDecimal("0.0"), BigDecimal("0.001"), BigDecimal("0.0002"), BigDecimal("-0.0001"), BigDecimal("0.5"), BigDecimal("0.4"), BigDecimal("0.3"), BigDecimal("-0.45"), BigDecimal("0.02"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 1), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.history_trade_greeks_all(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryTradeGreeksAll
      end
    end

    it "returns TradeGreeksAllRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.history_trade_greeks_all(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::TradeGreeksAllRow)
        expect(result.first.price).to be == BigDecimal("5.25")
        expect(result.first.delta).to be == BigDecimal("0.55")
      end
    end
  end

  with ".history_trade_greeks_first_order" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp sequence ext_condition1 ext_condition2 ext_condition3 ext_condition4 condition size exchange price delta theta vega rho epsilon lambda implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 9, 30, 0), 12345, 0, 0, 0, 0, 0, 10, 1, BigDecimal("5.25"), BigDecimal("0.55"), BigDecimal("-0.05"), BigDecimal("0.20"), BigDecimal("0.10"), BigDecimal("-0.08"), BigDecimal("15.5"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 1), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.history_trade_greeks_first_order(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryTradeGreeksFirstOrder
      end
    end

    it "returns TradeGreeksFirstOrderRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.history_trade_greeks_first_order(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::TradeGreeksFirstOrderRow)
      end
    end
  end

  with ".history_trade_greeks_second_order" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp sequence ext_condition1 ext_condition2 ext_condition3 ext_condition4 condition size exchange price gamma vanna charm vomma veta implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 9, 30, 0), 12345, 0, 0, 0, 0, 0, 10, 1, BigDecimal("5.25"), BigDecimal("0.02"), BigDecimal("0.01"), BigDecimal("-0.005"), BigDecimal("0.5"), BigDecimal("0.1"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 1), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.history_trade_greeks_second_order(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryTradeGreeksSecondOrder
      end
    end

    it "returns TradeGreeksSecondOrderRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.history_trade_greeks_second_order(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::TradeGreeksSecondOrderRow)
      end
    end
  end

  with ".history_trade_greeks_third_order" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp sequence ext_condition1 ext_condition2 ext_condition3 ext_condition4 condition size exchange price speed zomma color ultima implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 9, 30, 0), 12345, 0, 0, 0, 0, 0, 10, 1, BigDecimal("5.25"), BigDecimal("0.001"), BigDecimal("0.0002"), BigDecimal("-0.0001"), BigDecimal("0.5"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 1), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.history_trade_greeks_third_order(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryTradeGreeksThirdOrder
      end
    end

    it "returns TradeGreeksThirdOrderRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.history_trade_greeks_third_order(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::TradeGreeksThirdOrderRow)
      end
    end
  end

  with ".history_trade_greeks_implied_volatility" do
    let(:greeks_response) do
      {
        headers: %w[symbol expiration strike right timestamp sequence ext_condition1 ext_condition2 ext_condition3 ext_condition4 condition size exchange price implied_vol iv_error underlying_timestamp underlying_price],
        rows: [["AAPL", "2025-01-17", BigDecimal("150.00"), "CALL", Time.new(2024, 12, 1, 9, 30, 0), 12345, 0, 0, 0, 0, 0, 10, 1, BigDecimal("5.25"), BigDecimal("0.25"), BigDecimal("-0.001"), Time.new(2024, 12, 1), BigDecimal("155.00")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(greeks_response) do |conn|
        ThetaData::REST::Option.history_trade_greeks_implied_volatility(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(conn.last_call[:method]).to be == :GetOptionHistoryTradeGreeksImpliedVolatility
      end
    end

    it "returns TradeGreeksImpliedVolatilityRow Data objects" do
      with_mock_connection(greeks_response) do |conn|
        result = ThetaData::REST::Option.history_trade_greeks_implied_volatility(symbol: "AAPL", expiration: "2025-01-17", date: Date.new(2024, 12, 1))
        expect(result).to be_a(Array)
        expect(result.first).to be_a(ThetaData::REST::TradeGreeksImpliedVolatilityRow)
      end
    end
  end
end
