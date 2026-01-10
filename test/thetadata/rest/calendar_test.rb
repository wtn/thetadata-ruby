require "thetadata"
require "date"

describe ThetaData::REST::Calendar do
  def make_mock_session
    ThetaData::REST::Session.new(
      session_id: "test-session-id",
      user: {},
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

  with ".today" do
    let(:today_response) do
      {
        headers: %w[type open close],
        rows: [["open", "09:30:00", "16:00:00"]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(today_response) do |conn|
        ThetaData::REST::Calendar.today
        expect(conn.last_call[:method]).to be == :GetCalendarOpenToday
      end
    end

    it "returns a CalendarDayRow" do
      with_mock_connection(today_response) do |conn|
        result = ThetaData::REST::Calendar.today
        expect(result).to be_a(ThetaData::REST::CalendarDayRow)
        expect(result.type).to be == "open"
        expect(result.open).to be == "09:30:00"
        expect(result.close).to be == "16:00:00"
      end
    end

    it "handles weekend response" do
      weekend_response = {
        headers: %w[type open close],
        rows: [["weekend", nil, nil]],
      }
      with_mock_connection(weekend_response) do |conn|
        result = ThetaData::REST::Calendar.today
        expect(result.type).to be == "weekend"
        expect(result.open).to be_nil
        expect(result.close).to be_nil
      end
    end
  end

  with ".on_date" do
    let(:date_response) do
      {
        headers: %w[type open close],
        rows: [["early_close", "09:30:00", "13:00:00"]],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(date_response) do |conn|
        ThetaData::REST::Calendar.on_date(Date.new(2024, 12, 24))
        expect(conn.last_call[:method]).to be == :GetCalendarOnDate
      end
    end

    it "returns a CalendarDayRow" do
      with_mock_connection(date_response) do |conn|
        result = ThetaData::REST::Calendar.on_date(Date.new(2024, 12, 24))
        expect(result).to be_a(ThetaData::REST::CalendarDayRow)
        expect(result.type).to be == "early_close"
        expect(result.open).to be == "09:30:00"
        expect(result.close).to be == "13:00:00"
      end
    end

    it "handles full_close response" do
      closed_response = {
        headers: %w[type open close],
        rows: [["full_close", nil, nil]],
      }
      with_mock_connection(closed_response) do |conn|
        result = ThetaData::REST::Calendar.on_date(Date.new(2024, 12, 25))
        expect(result.type).to be == "full_close"
        expect(result.open).to be_nil
        expect(result.close).to be_nil
      end
    end
  end

  with ".year" do
    let(:year_response) do
      {
        headers: %w[date type open close],
        rows: [
          ["2024-01-01", "full_close", nil, nil],
          ["2024-01-15", "full_close", nil, nil],
          ["2024-02-19", "full_close", nil, nil],
          ["2024-03-29", "full_close", nil, nil],
          ["2024-05-27", "full_close", nil, nil],
          ["2024-06-19", "full_close", nil, nil],
          ["2024-07-03", "early_close", "09:30:00", "13:00:00"],
          ["2024-07-04", "full_close", nil, nil],
          ["2024-09-02", "full_close", nil, nil],
          ["2024-11-28", "full_close", nil, nil],
          ["2024-11-29", "early_close", "09:30:00", "13:00:00"],
          ["2024-12-24", "early_close", "09:30:00", "13:00:00"],
          ["2024-12-25", "full_close", nil, nil],
        ],
      }
    end

    it "calls connection with correct method" do
      with_mock_connection(year_response) do |conn|
        ThetaData::REST::Calendar.year(2024)
        expect(conn.last_call[:method]).to be == :GetCalendarYear
      end
    end

    it "returns array of CalendarYearRow" do
      with_mock_connection(year_response) do |conn|
        result = ThetaData::REST::Calendar.year(2024)
        expect(result).to be_a(Array)
        expect(result.length).to be == 13
        expect(result.first).to be_a(ThetaData::REST::CalendarYearRow)
      end
    end

    it "correctly parses holiday data" do
      with_mock_connection(year_response) do |conn|
        result = ThetaData::REST::Calendar.year(2024)

        # Check full_close holiday
        new_years = result.find { |r| r.date == "2024-01-01" }
        expect(new_years.type).to be == "full_close"
        expect(new_years.open).to be_nil
        expect(new_years.close).to be_nil

        # Check early_close holiday
        christmas_eve = result.find { |r| r.date == "2024-12-24" }
        expect(christmas_eve.type).to be == "early_close"
        expect(christmas_eve.open).to be == "09:30:00"
        expect(christmas_eve.close).to be == "13:00:00"
      end
    end

    it "accepts year as integer or string" do
      with_mock_connection(year_response) do |conn|
        ThetaData::REST::Calendar.year(2024)
        expect(conn.last_call[:request].year).to be == "2024"
      end
    end
  end
end
