module ThetaData
  class Error < StandardError
  end

  class AuthenticationError < Error
  end

  class SessionExpiredError < AuthenticationError
  end

  class ConnectionError < Error
  end

  class ServerError < Error
    attr_reader :grpc_status

    def initialize(message = nil, grpc_status: nil)
      @grpc_status = grpc_status
      super(message)
    end
  end

  class NotFoundError < Error
  end

  class RateLimitError < Error
    attr_reader :retry_after

    def initialize(message = nil, retry_after: nil)
      @retry_after = retry_after
      super(message)
    end
  end

  class SubscriptionError < Error
  end

  class TimeoutError < Error
  end
end
