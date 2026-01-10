module ThetaData
  module REST
    def self.midpoint_value(bid, ask)
      if bid.is_a?(Integer) && ask.is_a?(Integer)
        (bid / 2) + (ask / 2) + (((bid % 2) + (ask % 2)) / 2)
      else
        (bid + ask) / 2
      end
    end

    def self.format_date(date)
      return nil if date.nil?

      unless date.is_a?(Date)
        raise ArgumentError, "Expected Date, got #{date.class}"
      end

      date.strftime("%Y%m%d")
    end

    class << self
      def connection
        @connection ||= begin
          config = ThetaData.configuration
          if config.email.nil? || config.email.empty?
            raise AuthenticationError, "Missing email - set THETADATA_ACCOUNT_EMAIL or configure ThetaData.configuration.email"
          end
          if config.password.nil? || config.password.empty?
            raise AuthenticationError, "Missing password - set THETADATA_ACCOUNT_PASSWORD or configure ThetaData.configuration.password"
          end
          conn = Connection.new(email: config.email, password: config.password)
          conn.authenticate!
          conn
        end
      end

      def close
        @connection&.close
        @connection = nil
      end
    end
  end
end
