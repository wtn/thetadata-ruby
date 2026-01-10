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
  end
end
