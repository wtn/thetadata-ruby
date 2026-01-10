require "thetadata"

describe ThetaData::REST::Connection do
  let(:email) { "test@example.com" }
  let(:password) { "password123" }
  let(:connection) { ThetaData::REST::Connection.new(email: email, password: password) }

  it "stores email" do
    expect(connection.email).to be == email
  end

  it "stores password" do
    expect(connection.password).to be == password
  end

  it "starts without session" do
    expect(connection.session).to be == nil
  end

  with "constants" do
    it "has MAX_RETRIES of 3" do
      expect(ThetaData::REST::Connection::MAX_RETRIES).to be == 3
    end

    it "has BASE_DELAY of 0.5" do
      expect(ThetaData::REST::Connection::BASE_DELAY).to be == 0.5
    end

    it "has MAX_DELAY of 8.0" do
      expect(ThetaData::REST::Connection::MAX_DELAY).to be == 8.0
    end

    it "defines TRANSIENT_ERRORS" do
      errors = ThetaData::REST::Connection::TRANSIENT_ERRORS
      expect(errors).to be(:include?, Protocol::GRPC::Unavailable)
      expect(errors).to be(:include?, Protocol::GRPC::DeadlineExceeded)
      expect(errors).to be(:include?, Protocol::GRPC::Internal)
      expect(errors).to be(:include?, EOFError)
      expect(errors).to be(:include?, IOError)
      expect(errors).to be(:include?, SocketError)
      expect(errors).to be(:include?, Errno::ECONNRESET)
      expect(errors).to be(:include?, Errno::ECONNREFUSED)
      expect(errors).to be(:include?, Errno::ETIMEDOUT)
    end
  end

  with "#authenticated?" do
    it "returns false when no session" do
      expect(connection.authenticated?).to be == false
    end

    it "returns false when session is expired" do
      expired_session = ThetaData::REST::Session.new(
        session_id: "test-id",
        user: {},
        created_at: Time.now - 4000,
      )
      connection.instance_variable_set(:@session, expired_session)

      expect(connection.authenticated?).to be == false
    end

    it "returns true when session is valid" do
      valid_session = ThetaData::REST::Session.new(
        session_id: "test-id",
        user: {},
      )
      connection.instance_variable_set(:@session, valid_session)

      expect(connection.authenticated?).to be == true
    end
  end

  with "#invalidate_session!" do
    it "clears the session" do
      valid_session = ThetaData::REST::Session.new(
        session_id: "test-id",
        user: {},
      )
      connection.instance_variable_set(:@session, valid_session)

      connection.send(:invalidate_session!)

      expect(connection.session).to be == nil
    end
  end

  with "#update_max_concurrency!" do
    it "sets max_concurrency based on subscription tier" do
      session = ThetaData::REST::Session.new(
        session_id: "test-id",
        user: { stockSubscription: "STANDARD", optionsSubscription: "VALUE" },
      )
      connection.instance_variable_set(:@session, session)

      connection.send(:update_max_concurrency!)

      expect(ThetaData.configuration.max_concurrency).to be == 4
    ensure
      ThetaData.configuration.max_concurrency = 1
    end

    it "sets pro tier for highest subscription" do
      session = ThetaData::REST::Session.new(
        session_id: "test-id",
        user: { indicesSubscription: 3 },
      )
      connection.instance_variable_set(:@session, session)

      connection.send(:update_max_concurrency!)

      expect(ThetaData.configuration.max_concurrency).to be == 8
    ensure
      ThetaData.configuration.max_concurrency = 1
    end

    it "defaults to 1 for unknown tier" do
      session = ThetaData::REST::Session.new(
        session_id: "test-id",
        user: { stockSubscription: "UNKNOWN" },
      )
      connection.instance_variable_set(:@session, session)

      connection.send(:update_max_concurrency!)

      expect(ThetaData.configuration.max_concurrency).to be == 1
    ensure
      ThetaData.configuration.max_concurrency = 1
    end
  end

  with "#reset_grpc_client!" do
    it "closes and clears the grpc client" do
      mock_client = Object.new
      closed = false
      mock_client.define_singleton_method(:close) { closed = true }

      connection.instance_variable_set(:@grpc_client, mock_client)
      connection.send(:reset_grpc_client!)

      expect(closed).to be == true
      expect(connection.instance_variable_get(:@grpc_client)).to be == nil
    end

    it "handles nil grpc client" do
      connection.instance_variable_set(:@grpc_client, nil)
      connection.send(:reset_grpc_client!)

      expect(connection.instance_variable_get(:@grpc_client)).to be == nil
    end
  end

  with "#sleep_with_backoff" do
    it "calculates exponential delay with jitter" do
      delays = []
      connection.define_singleton_method(:sleep) { |d| delays << d }

      connection.send(:sleep_with_backoff, 2)
      connection.send(:sleep_with_backoff, 3)
      connection.send(:sleep_with_backoff, 4)

      expect(delays[0]).to be >= 0.5
      expect(delays[0]).to be < 0.75
      expect(delays[1]).to be >= 1.0
      expect(delays[1]).to be < 1.5
      expect(delays[2]).to be >= 2.0
      expect(delays[2]).to be < 3.0
    end

    it "caps delay at MAX_DELAY" do
      delays = []
      connection.define_singleton_method(:sleep) { |d| delays << d }

      connection.send(:sleep_with_backoff, 100)

      expect(delays[0]).to be <= 12.0
    end
  end

  with "#semaphore" do
    it "returns an Async::Semaphore" do
      semaphore = connection.send(:semaphore)
      expect(semaphore).to be_a(Async::Semaphore)
    end

    it "uses max_concurrency from configuration" do
      ThetaData.configuration.max_concurrency = 4
      semaphore = connection.send(:semaphore)
      expect(semaphore.limit).to be == 4
    ensure
      ThetaData.configuration.max_concurrency = 1
    end

    it "memoizes the semaphore" do
      semaphore1 = connection.send(:semaphore)
      semaphore2 = connection.send(:semaphore)
      expect(semaphore1).to be == semaphore2
    end
  end

  with "#extract_value" do
    it "raises ServerError for unknown data type" do
      data_value = Object.new
      data_value.define_singleton_method(:data_type) { :unknown_type }

      expect do
        connection.send(:extract_value, data_value)
      end.to raise_exception(ThetaData::ServerError, message: be =~ /Unknown data type/)
    end
  end

  with "integration" do
    def skip_unless_live
      unless ENV["THETADATA_EMAIL"]
        skip "Live API tests require THETADATA_EMAIL env var"
      end
    end

    it "can authenticate with real credentials" do
      skip_unless_live

      connection = ThetaData::REST::Connection.new(
        email: ENV["THETADATA_EMAIL"],
        password: ENV["THETADATA_PASSWORD"],
      )

      Async do
        connection.authenticate!
        expect(connection.authenticated?).to be == true
        expect(connection.session).to be_a(ThetaData::REST::Session)
      end.wait
    end
  end
end
