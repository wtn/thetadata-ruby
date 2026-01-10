module ThetaData
  module REST
    module InterestRate
      class << self
        # Get end-of-day interest rate history for a benchmark symbol.
        # Returns rows as hashes keyed by column name; the server-defined schema isn't
        # mirrored in a Data class yet because we haven't pinned it down with live data.
        def history_eod(symbol, start_date:, end_date:)
          request = ::Endpoints::InterestRateHistoryEodRequest.new(
            query_info: query_info,
            params: ::Endpoints::InterestRateHistoryEodRequestQuery.new(
              symbol: symbol,
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
            ),
          )

          response = connection.call(:GetInterestRateHistoryEod, request)
          headers = response[:headers].map { |h| h.downcase.to_sym }
          response[:rows].map { |row| headers.zip(row).to_h }
        end

        private

        def connection
          REST.connection
        end

        def query_info
          connection.query_info
        end
      end
    end
  end
end
