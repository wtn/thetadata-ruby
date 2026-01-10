require "thetadata"

describe ThetaData::Error do
  it "inherits from StandardError" do
    expect(ThetaData::Error.ancestors).to be(:include?, StandardError)
  end
end

describe ThetaData::AuthenticationError do
  it "inherits from Error" do
    expect(ThetaData::AuthenticationError.ancestors).to be(:include?, ThetaData::Error)
  end
end

describe ThetaData::SessionExpiredError do
  it "inherits from AuthenticationError" do
    expect(ThetaData::SessionExpiredError.ancestors).to be(:include?, ThetaData::AuthenticationError)
  end
end

describe ThetaData::ConnectionError do
  it "inherits from Error" do
    expect(ThetaData::ConnectionError.ancestors).to be(:include?, ThetaData::Error)
  end
end

describe ThetaData::ServerError do
  it "inherits from Error" do
    expect(ThetaData::ServerError.ancestors).to be(:include?, ThetaData::Error)
  end

  with "grpc_status" do
    let(:error) { ThetaData::ServerError.new("Server error", grpc_status: 13) }

    it "stores grpc_status" do
      expect(error.grpc_status).to be == 13
    end
  end
end

describe ThetaData::NotFoundError do
  it "inherits from Error" do
    expect(ThetaData::NotFoundError.ancestors).to be(:include?, ThetaData::Error)
  end
end

describe ThetaData::RateLimitError do
  it "inherits from Error" do
    expect(ThetaData::RateLimitError.ancestors).to be(:include?, ThetaData::Error)
  end

  with "retry_after" do
    let(:error) { ThetaData::RateLimitError.new("Rate limited", retry_after: 60) }

    it "stores retry_after" do
      expect(error.retry_after).to be == 60
    end
  end
end

describe ThetaData::SubscriptionError do
  it "inherits from Error" do
    expect(ThetaData::SubscriptionError.ancestors).to be(:include?, ThetaData::Error)
  end
end

describe ThetaData::TimeoutError do
  it "inherits from Error" do
    expect(ThetaData::TimeoutError.ancestors).to be(:include?, ThetaData::Error)
  end
end
