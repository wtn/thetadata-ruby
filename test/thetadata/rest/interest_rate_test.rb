require "thetadata"
require "date"

describe ThetaData::REST::InterestRate do
  def make_mock_session
    ThetaData::REST::Session.new(
      session_id: "test-session-id",
      user: { stocksSubscription: "PRO" },
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
        headers: %w[date rate],
        rows: [
          [Date.new(2024, 12, 1), BigDecimal("4.25")],
          [Date.new(2024, 12, 2), BigDecimal("4.26")],
        ],
      }
    end

    it "calls connection with GetInterestRateHistoryEod" do
      with_mock_connection(eod_response) do |conn|
        ThetaData::REST::InterestRate.history_eod("SOFR", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 2))
        expect(conn.last_call[:method]).to be == :GetInterestRateHistoryEod
      end
    end

    it "passes symbol and date range through to the request" do
      with_mock_connection(eod_response) do |conn|
        ThetaData::REST::InterestRate.history_eod("SOFR", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 2))
        params = conn.last_call[:request].params
        expect(params.symbol).to be == "SOFR"
        expect(params.start_date).to be == "2024-12-01"
        expect(params.end_date).to be == "2024-12-02"
      end
    end

    it "returns an array of header-keyed hashes" do
      with_mock_connection(eod_response) do |conn|
        result = ThetaData::REST::InterestRate.history_eod("SOFR", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 2))
        expect(result).to be_a(Array)
        expect(result.length).to be == 2
        expect(result.first).to be == {date: Date.new(2024, 12, 1), rate: BigDecimal("4.25")}
      end
    end
  end
end
