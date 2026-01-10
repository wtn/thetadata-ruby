require "thetadata"
require "async"
require "stringio"

describe ThetaData::Streaming do
  def create_mock_client_with_events(events)
    stream = StringIO.new
    events.each do |event|
      case event[:type]
      when :metadata
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::METADATA, event[:subscriptions] || "STOCK.FREE").write(stream)
      when :ping
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(stream)
      when :error
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::ERROR, event[:message] || "").write(stream)
      when :disconnected
        payload = event[:reason_code] ? [event[:reason_code]].pack("S>") : ""
        Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::DISCONNECTED, payload).write(stream)
      end
    end
    stream.rewind

    connection = ThetaData::Streaming::Connection.new(stream)
    ThetaData::Streaming::Client.new(connection)
  end

  with ".wait_for_metadata" do
    it "returns metadata event when received" do
      client = create_mock_client_with_events([
        { type: :metadata, subscriptions: "STOCK.FREE, INDEX.PRO" },
      ])

      Sync do
        event = ThetaData::Streaming.send(:wait_for_metadata, client)

        expect(event[:type]).to be == :metadata
        expect(event[:subscriptions]).to be == { stock: :free, index: :pro }
      end
    end

    it "skips non-metadata events before finding metadata" do
      client = create_mock_client_with_events([
        { type: :ping },
        { type: :ping },
        { type: :metadata, subscriptions: "STOCK.FREE" },
      ])

      Sync do
        event = ThetaData::Streaming.send(:wait_for_metadata, client)

        expect(event[:type]).to be == :metadata
      end
    end

    it "raises AuthenticationError when connection closes before metadata" do
      client = create_mock_client_with_events([])

      Sync do
        expect {
          ThetaData::Streaming.send(:wait_for_metadata, client)
        }.to raise_exception(ThetaData::AuthenticationError, message: be(:include?, "Connection closed"))
      end
    end

    it "raises AuthenticationError on error event" do
      client = create_mock_client_with_events([
        { type: :error, message: "Invalid credentials" },
      ])

      Sync do
        expect {
          ThetaData::Streaming.send(:wait_for_metadata, client)
        }.to raise_exception(ThetaData::AuthenticationError)
      end
    end

    it "raises AuthenticationError on disconnected event" do
      client = create_mock_client_with_events([
        { type: :disconnected, reason_code: 1 },
      ])

      Sync do
        expect {
          ThetaData::Streaming.send(:wait_for_metadata, client)
        }.to raise_exception(ThetaData::AuthenticationError)
      end
    end

    it "raises AuthenticationError on timeout" do
      read_io, write_io = IO.pipe

      connection = ThetaData::Streaming::Connection.new(read_io)
      client = ThetaData::Streaming::Client.new(connection)

      Sync do
        expect {
          ThetaData::Streaming.send(:wait_for_metadata, client, timeout: 0.1)
        }.to raise_exception(ThetaData::AuthenticationError, message: be(:include?, "timed out"))
      end
    ensure
      read_io&.close
      write_io&.close
    end

    it "accepts custom timeout parameter" do
      client = create_mock_client_with_events([
        { type: :metadata, subscriptions: "STOCK.FREE" },
      ])

      Sync do
        event = ThetaData::Streaming.send(:wait_for_metadata, client, timeout: 5)

        expect(event[:type]).to be == :metadata
      end
    end
  end

  with ".validate_credentials!" do
    it "raises AuthenticationError when email is nil" do
      original_email = ThetaData.configuration.email
      ThetaData.configuration.email = nil

      expect {
        ThetaData::Streaming.send(:validate_credentials!)
      }.to raise_exception(ThetaData::AuthenticationError, message: be(:include?, "email"))
    ensure
      ThetaData.configuration.email = original_email
    end

    it "raises AuthenticationError when email is empty" do
      original_email = ThetaData.configuration.email
      ThetaData.configuration.email = ""

      expect {
        ThetaData::Streaming.send(:validate_credentials!)
      }.to raise_exception(ThetaData::AuthenticationError, message: be(:include?, "email"))
    ensure
      ThetaData.configuration.email = original_email
    end

    it "raises AuthenticationError when password is nil" do
      original_email = ThetaData.configuration.email
      original_password = ThetaData.configuration.password
      ThetaData.configuration.email = "test@example.com"
      ThetaData.configuration.password = nil

      expect {
        ThetaData::Streaming.send(:validate_credentials!)
      }.to raise_exception(ThetaData::AuthenticationError, message: be(:include?, "password"))
    ensure
      ThetaData.configuration.email = original_email
      ThetaData.configuration.password = original_password
    end

    it "raises AuthenticationError when password is empty" do
      original_email = ThetaData.configuration.email
      original_password = ThetaData.configuration.password
      ThetaData.configuration.email = "test@example.com"
      ThetaData.configuration.password = ""

      expect {
        ThetaData::Streaming.send(:validate_credentials!)
      }.to raise_exception(ThetaData::AuthenticationError, message: be(:include?, "password"))
    ensure
      ThetaData.configuration.email = original_email
      ThetaData.configuration.password = original_password
    end

    it "does not raise when credentials are present" do
      original_email = ThetaData.configuration.email
      original_password = ThetaData.configuration.password
      ThetaData.configuration.email = "test@example.com"
      ThetaData.configuration.password = "secret"

      ThetaData::Streaming.send(:validate_credentials!)
    ensure
      ThetaData.configuration.email = original_email
      ThetaData.configuration.password = original_password
    end
  end
end
