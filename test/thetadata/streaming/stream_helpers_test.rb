require "thetadata"
require "async"
require "stringio"

# Helper class to test StreamHelpers
class StreamHelpersTester
  include ThetaData::Streaming::StreamHelpers
end

describe ThetaData::Streaming::StreamHelpers do
  def create_mock_client_with_events(events)
    stream = StringIO.new
    events.each do |event|
      case event[:type]
      when :req_response
        payload = [event[:request_id]].pack("N") + [event[:response_code] || 0].pack("N")
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::REQ_RESPONSE, payload).write(stream)
      when :metadata
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::METADATA, event[:subscriptions] || "STOCK.FREE").write(stream)
      when :ping
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(stream)
      when :trade
        fit_data = "\x10\x0D"  # contract_id=100
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE, fit_data).write(stream)
      end
    end
    stream.rewind

    connection = ThetaData::Streaming::Connection.new(stream)
    ThetaData::Streaming::Client.new(connection)
  end

  let(:helper) { StreamHelpersTester.new }

  with "#wait_for_subscription" do
    it "returns when subscription is confirmed" do
      client = create_mock_client_with_events([
        { type: :req_response, request_id: 1, response_code: 0 },  # subscribed
      ])

      Sync do
        helper.wait_for_subscription(client, 1)
      end
    end

    it "skips non-matching events before finding subscription response" do
      client = create_mock_client_with_events([
        { type: :ping },
        { type: :metadata, subscriptions: "STOCK.FREE" },
        { type: :req_response, request_id: 1, response_code: 0 },
      ])

      Sync do
        helper.wait_for_subscription(client, 1)
      end
    end

    it "raises SubscriptionError when subscription fails" do
      client = create_mock_client_with_events([
        { type: :req_response, request_id: 1, response_code: 1 },  # error
      ])

      Sync do
        expect {
          helper.wait_for_subscription(client, 1)
        }.to raise_exception(ThetaData::SubscriptionError)
      end
    end

    it "raises SubscriptionError when connection closes" do
      client = create_mock_client_with_events([])  # Empty - will return nil

      Sync do
        expect {
          helper.wait_for_subscription(client, 1)
        }.to raise_exception(ThetaData::SubscriptionError, message: be(:include?, "Connection closed"))
      end
    end

    it "raises SubscriptionError on timeout" do
      read_io, write_io = IO.pipe

      connection = ThetaData::Streaming::Connection.new(read_io)
      client = ThetaData::Streaming::Client.new(connection)

      Sync do
        expect {
          helper.wait_for_subscription(client, 1, timeout: 0.1)
        }.to raise_exception(ThetaData::SubscriptionError, message: be(:include?, "timed out"))
      end
    ensure
      read_io&.close
      write_io&.close
    end

    it "accepts custom timeout parameter" do
      client = create_mock_client_with_events([
        { type: :req_response, request_id: 1, response_code: 0 },
      ])

      Sync do
        helper.wait_for_subscription(client, 1, timeout: 5)
      end
    end
  end

  with "#wait_for_subscriptions" do
    it "waits for multiple subscription responses" do
      client = create_mock_client_with_events([
        { type: :req_response, request_id: 1, response_code: 0 },
        { type: :req_response, request_id: 2, response_code: 0 },
      ])

      Sync do
        helper.wait_for_subscriptions(client, { 1 => "SPX", 2 => "AAPL" })
      end
    end

    it "raises SubscriptionError if any subscription fails" do
      client = create_mock_client_with_events([
        { type: :req_response, request_id: 1, response_code: 0 },
        { type: :req_response, request_id: 2, response_code: 1 },
      ])

      Sync do
        expect {
          helper.wait_for_subscriptions(client, { 1 => "SPX", 2 => "AAPL" })
        }.to raise_exception(ThetaData::SubscriptionError)
      end
    end

    it "raises SubscriptionError on timeout" do
      read_io, write_io = IO.pipe

      connection = ThetaData::Streaming::Connection.new(read_io)
      client = ThetaData::Streaming::Client.new(connection)

      Sync do
        expect {
          helper.wait_for_subscriptions(client, { 1 => "SPX" }, timeout: 0.1)
        }.to raise_exception(ThetaData::SubscriptionError, message: be(:include?, "timed out"))
      end
    ensure
      read_io&.close
      write_io&.close
    end
  end

  with "#stream_events" do
    it "yields events of the specified type" do
      client = create_mock_client_with_events([
        { type: :ping },
        { type: :trade },
        { type: :ping },
      ])

      events = []
      helper.stream_events(client, :ping) do |event|
        events << event
      end

      expect(events.length).to be == 2
      expect(events.first[:type]).to be == :ping
    end

    it "returns enumerator without block" do
      client = create_mock_client_with_events([
        { type: :trade },
        { type: :trade },
      ])

      enum = helper.stream_events(client, :trade)

      expect(enum).to be_a(Enumerator)
      expect(enum.first[:type]).to be == :trade
    end
  end
end
