require "thetadata"
require "date"

describe ThetaData::REST::Index do
  def make_mock_session
    ThetaData::REST::Session.new(
      session_id: "test-session-id",
      user: { indicesSubscription: "PRO" },
    )
  end

  def make_mock_connection(response_data)
    session = make_mock_session
    qi = ::Endpoints::QueryInfo.new(
      auth_token: ::Endpoints::AuthToken.new(session_uuid: session.session_id),
    )
    Object.new.tap do |conn|
      conn.define_singleton_method(:session) { session }
      conn.define_singleton_method(:query_info) { qi }
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

  with ".history_eod" do
    let(:eod_response) do
      {
        headers: %w[created last_trade open high low close volume count bid_size bid_exchange bid bid_condition ask_size ask_exchange ask ask_condition],
        rows: [[Time.new(2024, 12, 1), Time.new(2024, 12, 1), 5893.50, 5920.00, 5880.00, 5910.25, 1000, 50, 10, 1, 5910.00, 0, 15, 1, 5911.00, 0]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(eod_response) do |conn|
        ThetaData::REST::Index.history_eod("SPX", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31))
        expect(conn.last_call[:method]).to be == :GetIndexHistoryEod
      end
    end

    it "returns EODRow Data objects" do
      with_mock_connection(eod_response) do |conn|
        result = ThetaData::REST::Index.history_eod("SPX", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 31))
        expect(result).to be_a(Array)
        expect(result.length).to be == 1
        expect(result.first).to be_a(ThetaData::REST::EODRow)
        expect(result.first.open).to be == 5893.50
        expect(result.first.close).to be == 5910.25
      end
    end
  end

  with ".history_ohlc" do
    let(:ohlc_response) do
      {
        headers: %w[timestamp open high low close volume count vwap],
        rows: [[Time.new(2026, 4, 10, 9, 30), BigDecimal("6839.24"), BigDecimal("6845.77"), BigDecimal("6830.00"), BigDecimal("6840.00"), 0, 0, BigDecimal("0")]],
      }
    end

    it "formats interval as shorthand" do
      with_mock_connection(ohlc_response) do |conn|
        ThetaData::REST::Index.history_ohlc("SPX", start_date: Date.new(2026, 4, 10), end_date: Date.new(2026, 4, 10), interval: 60000)
        params = conn.last_call[:request].params
        expect(params.interval).to be == "1m"
      end
    end

    it "formats start_time with milliseconds" do
      with_mock_connection(ohlc_response) do |conn|
        ThetaData::REST::Index.history_ohlc("SPX", start_date: Date.new(2026, 4, 10), end_date: Date.new(2026, 4, 10), interval: "1m", start_time: "09:30:00")
        params = conn.last_call[:request].params
        expect(params.start_time).to be == "09:30:00.000"
      end
    end

    it "passes string intervals through" do
      with_mock_connection(ohlc_response) do |conn|
        ThetaData::REST::Index.history_ohlc("SPX", start_date: Date.new(2026, 4, 10), end_date: Date.new(2026, 4, 10), interval: "15m")
        params = conn.last_call[:request].params
        expect(params.interval).to be == "15m"
      end
    end
  end

  with ".history_price" do
    let(:price_tick_response) do
      {
        headers: %w[timestamp price],
        rows: [[Time.new(2026, 4, 10, 9, 30, 1), BigDecimal("6839.24")]],
      }
    end

    it "formats interval as shorthand" do
      with_mock_connection(price_tick_response) do |conn|
        ThetaData::REST::Index.history_price("SPX", date: Date.new(2026, 4, 10), interval: 1000)
        params = conn.last_call[:request].params
        expect(params.interval).to be == "1s"
      end
    end

    it "formats times with milliseconds" do
      with_mock_connection(price_tick_response) do |conn|
        ThetaData::REST::Index.history_price("SPX", date: Date.new(2026, 4, 10), interval: "1s", start_time: "09:00:00", end_time: "16:30:00")
        params = conn.last_call[:request].params
        expect(params.start_time).to be == "09:00:00.000"
        expect(params.end_time).to be == "16:30:00.000"
      end
    end

    it "returns PriceRow Data objects" do
      with_mock_connection(price_tick_response) do |conn|
        result = ThetaData::REST::Index.history_price("SPX", date: Date.new(2026, 4, 10), interval: 1000)
        expect(result.first).to be_a(ThetaData::REST::PriceRow)
        expect(result.first.price).to be == BigDecimal("6839.24")
      end
    end

    it "sends tick interval for every-change tick data (interval: 0)" do
      with_mock_connection(price_tick_response) do |conn|
        ThetaData::REST::Index.history_price("SPX", date: Date.new(2026, 4, 10), interval: 0)
        params = conn.last_call[:request].params
        expect(params.interval).to be == "tick"
      end
    end

    it "passes tick string interval through" do
      with_mock_connection(price_tick_response) do |conn|
        ThetaData::REST::Index.history_price("SPX", date: Date.new(2026, 4, 10), interval: "tick")
        params = conn.last_call[:request].params
        expect(params.interval).to be == "tick"
      end
    end

    it "passes start_date/end_date through for multi-day range" do
      with_mock_connection(price_tick_response) do |conn|
        ThetaData::REST::Index.history_price("SPX", start_date: Date.new(2026, 4, 1), end_date: Date.new(2026, 4, 30), interval: "1m")
        params = conn.last_call[:request].params
        expect(params.start_date).to be == "2026-04-01"
        expect(params.end_date).to be == "2026-04-30"
      end
    end
  end

  with ".history_ohlc tick interval" do
    let(:ohlc_response) do
      {
        headers: %w[timestamp open high low close volume count vwap],
        rows: [[Time.new(2026, 4, 10, 9, 30), BigDecimal("6839.24"), BigDecimal("6845.77"), BigDecimal("6830.00"), BigDecimal("6840.00"), 0, 0, BigDecimal("0")]],
      }
    end

    it "sends tick interval for every-change data (interval: 0)" do
      with_mock_connection(ohlc_response) do |conn|
        ThetaData::REST::Index.history_ohlc("SPX", start_date: Date.new(2026, 4, 10), end_date: Date.new(2026, 4, 10), interval: 0)
        params = conn.last_call[:request].params
        expect(params.interval).to be == "tick"
      end
    end
  end

  with ".history_ticks" do
    let(:tick_response) do
      {
        headers: %w[timestamp price],
        rows: [
          [Time.new(2026, 4, 10, 9, 30, 1), BigDecimal("6839.24")],
          [Time.new(2026, 4, 10, 9, 30, 2), BigDecimal("6841.33")],
        ],
      }
    end

    it "delegates to history_price with interval tick" do
      with_mock_connection(tick_response) do |conn|
        ThetaData::REST::Index.history_ticks("SPX", date: Date.new(2026, 4, 10))
        params = conn.last_call[:request].params
        expect(params.interval).to be == "tick"
      end
    end

    it "passes start_time and end_time through" do
      with_mock_connection(tick_response) do |conn|
        ThetaData::REST::Index.history_ticks("SPX", date: Date.new(2026, 4, 10), start_time: "08:30:00", end_time: "17:00:00")
        params = conn.last_call[:request].params
        expect(params.start_time).to be == "08:30:00.000"
        expect(params.end_time).to be == "17:00:00.000"
      end
    end

    it "returns PriceRow objects" do
      with_mock_connection(tick_response) do |conn|
        result = ThetaData::REST::Index.history_ticks("SPX", date: Date.new(2026, 4, 10))
        expect(result.length).to be == 2
        expect(result.first).to be_a(ThetaData::REST::PriceRow)
        expect(result.first.price).to be == BigDecimal("6839.24")
      end
    end
  end

  with ".snapshot_market_value" do
    let(:market_value_response) do
      {
        headers: %w[timestamp symbol market_price],
        rows: [[Time.new(2024, 12, 2, 16, 2, 6), "SPX", BigDecimal("5910.50")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(market_value_response) do |conn|
        ThetaData::REST::Index.snapshot_market_value("SPX")
        expect(conn.last_call[:method]).to be == :GetIndexSnapshotMarketValue
      end
    end

    it "returns single IndexSnapshotMarketValueRow for single symbol" do
      with_mock_connection(market_value_response) do |conn|
        result = ThetaData::REST::Index.snapshot_market_value("SPX")
        expect(result).to be_a(ThetaData::REST::IndexSnapshotMarketValueRow)
        expect(result.symbol).to be == "SPX"
        expect(result.market_price).to be == BigDecimal("5910.50")
      end
    end

    it "returns array for multiple symbols" do
      multi_response = {
        headers: %w[timestamp symbol market_price],
        rows: [
          [Time.new(2024, 12, 2), "SPX", BigDecimal("5910.50")],
          [Time.new(2024, 12, 2), "NDX", BigDecimal("20501.00")],
        ],
      }
      with_mock_connection(multi_response) do |conn|
        result = ThetaData::REST::Index.snapshot_market_value("SPX", "NDX")
        expect(result).to be_a(Array)
        expect(result.length).to be == 2
      end
    end
  end

  with ".snapshot_price" do
    let(:price_response) do
      {
        headers: ["timestamp", "symbol", "price"],
        rows: [[Time.new(2024, 12, 2, 16, 2, 6), "SPX", BigDecimal("5910.25")]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(price_response) do |conn|
        ThetaData::REST::Index.snapshot_price("SPX")
        expect(conn.last_call[:method]).to be == :GetIndexSnapshotPrice
      end
    end

    it "returns single SnapshotPriceRow for single symbol" do
      with_mock_connection(price_response) do |conn|
        result = ThetaData::REST::Index.snapshot_price("SPX")
        expect(result).to be_a(ThetaData::REST::SnapshotPriceRow)
        expect(result.price).to be == BigDecimal("5910.25")
        expect(result.symbol).to be == "SPX"
      end
    end

    it "passes min_time through to the request" do
      with_mock_connection(price_response) do |conn|
        ThetaData::REST::Index.snapshot_price("SPX", min_time: "10:00:00")
        params = conn.last_call[:request].params
        expect(params.min_time).to be == "10:00:00.000"
      end
    end
  end
end

describe ThetaData::REST::Index do
  with "integration" do
    def skip_unless_live
      unless ENV["THETADATA_ACCOUNT_EMAIL"]
        skip "Live API tests require THETADATA_ACCOUNT_EMAIL env var"
      end
    end

    it "fetches real SPX history" do
      skip_unless_live

      ThetaData.configure do |config|
        config.email = ENV["THETADATA_ACCOUNT_EMAIL"]
        config.password = ENV["THETADATA_ACCOUNT_PASSWORD"]
      end

      Async do
        result = ThetaData::REST::Index.history_eod("SPX", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 5))

        expect(result).to be_a(Array)
        expect(result.length).to be > 0
        expect(result.first).to be_a(ThetaData::REST::EODRow)
        expect(result.first.close).to be_a(BigDecimal)
      end.wait
    ensure
      ThetaData::REST.close
    end

    it "fetches tick data with interval: 0" do
      skip_unless_live

      ThetaData.configure do |config|
        config.email = ENV["THETADATA_ACCOUNT_EMAIL"]
        config.password = ENV["THETADATA_ACCOUNT_PASSWORD"]
      end

      Async do
        result = ThetaData::REST::Index.history_price("SPX",
          date: Date.new(2026, 4, 10),
          interval: 0,
          start_time: "09:30:00.000",
          end_time: "10:00:00.000",
        )

        expect(result).to be_a(Array)
        expect(result.length).to be > 100
        expect(result.first).to be_a(ThetaData::REST::PriceRow)
        expect(result.first.price).to be_a(BigDecimal)
        expect(result.first.price).to be > 0

        # Ticks should have irregular spacing (not every second filled)
        timestamps = result.map(&:timestamp)
        deltas = timestamps.each_cons(2).map {|a, b| b - a }
        expect(deltas.uniq.length).to be > 1
      end.wait
    ensure
      ThetaData::REST.close
    end
  end
end
