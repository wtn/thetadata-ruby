require "polars-df"

module ThetaData
  module REST
    def self.midpoint_value(bid, ask)
      if bid.is_a?(Integer) && ask.is_a?(Integer)
        (bid / 2) + (ask / 2) + (((bid % 2) + (ask % 2)) / 2)
      else
        (bid + ask) / 2
      end
    end

    # Convert an array of typed row objects (Data instances or plain hashes) into
    # a Polars::DataFrame. Mirrors the dataframe convenience the Python client
    # exposes via its `dataframe_type` constructor parameter.
    #
    # Accepts a single row, an array of rows, or nil (empty DataFrame).
    def self.to_polars(rows)
      rows = case rows
        when nil then []
        when Array then rows
        else [rows]
        end
      Polars::DataFrame.new(rows.map { |r| r.respond_to?(:to_h) ? r.to_h : r })
    end

    INTERVAL_MS_TO_SHORTHAND = {
      100 => "100ms", 500 => "500ms",
      1_000 => "1s", 5_000 => "5s", 10_000 => "10s", 15_000 => "15s", 30_000 => "30s",
      60_000 => "1m", 300_000 => "5m", 600_000 => "10m", 900_000 => "15m", 1_800_000 => "30m",
      3_600_000 => "1h",
    }.freeze

    def self.format_interval(interval)
      return interval.to_s if interval.is_a?(String)
      return "tick" if interval == 0

      INTERVAL_MS_TO_SHORTHAND.fetch(interval) do
        raise ArgumentError, "Unsupported interval: #{interval}ms. Use 0 for tick, or one of: #{INTERVAL_MS_TO_SHORTHAND.keys.join(", ")}, or a shorthand string."
      end
    end

    def self.format_time(time)
      return nil if time.nil?

      time = time.to_s
      time += ".000" unless time.include?(".")
      time
    end

    def self.format_date(date)
      return nil if date.nil?

      unless date.is_a?(Date)
        raise ArgumentError, "Expected Date, got #{date.class}"
      end

      date.strftime("%Y-%m-%d")
    end

    # History endpoints accept either a single `date:` *or* a `start_date:`/`end_date:`
    # range -- not both. Either form is OK; mixing them is rejected by the server,
    # so fail fast with a clear client-side error.
    def self.validate_date_range!(date, start_date, end_date)
      has_single = !date.nil?
      has_range = !start_date.nil? || !end_date.nil?

      if has_single && has_range
        raise ArgumentError, "Pass either date: for a single day or start_date:/end_date: for a range, not both"
      end

      if has_range && (start_date.nil? || end_date.nil?)
        raise ArgumentError, "start_date: and end_date: must be provided together"
      end
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
