module ThetaData
  module REST
    module Index
      class << self
        # List all available index symbols
        # @return [Array<String>] symbol names
        def list_symbols
          request = ::Endpoints::IndexListSymbolsRequest.new(
            auth_token: auth_token,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetIndexListSymbols, request)
          response[:rows].map(&:first)
        end

        # List available dates for given symbols
        # @return [Array<Date>] available dates
        def list_dates(*symbols)
          symbols = symbols.flatten
          request = ::Endpoints::IndexListDatesRequest.new(
            auth_token: auth_token,
            symbol: symbols,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetIndexListDates, request)
          response[:rows].map { |row| Date.strptime(row.first, "%Y-%m-%d") }
        end

        # Get current session OHLC for symbols
        def snapshot_ohlc(*symbols)
          symbols = symbols.flatten
          request = ::Endpoints::IndexSnapshotOhlcRequest.new(
            auth_token: auth_token,
            symbol: symbols,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetIndexSnapshotOhlc, request)
          result = rows_to_data(response, SnapshotOHLCRow)
          symbols.length == 1 ? result.first : result
        end

        # Get current price snapshot for symbols
        def snapshot_price(*symbols)
          symbols = symbols.flatten
          request = ::Endpoints::IndexSnapshotPriceRequest.new(
            auth_token: auth_token,
            symbol: symbols,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetIndexSnapshotPrice, request)
          result = rows_to_data(response, SnapshotPriceRow)
          symbols.length == 1 ? result.first : result
        end

        # Get end-of-day history
        def history_eod(symbol, start_date:, end_date:)
          request = ::Endpoints::IndexHistoryEodRequest.new(
            auth_token: auth_token,
            symbol: symbol,
            start_date: REST.format_date(start_date),
            end_date: REST.format_date(end_date),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetIndexHistoryEod, request)
          rows_to_data(response, EODRow)
        end

        # Get intraday OHLC bars
        def history_ohlc(symbol, start_date:, end_date:, interval:, start_time: "09:30:00", end_time: "16:00:00")
          request = ::Endpoints::IndexHistoryOhlcRequest.new(
            auth_token: auth_token,
            symbol: symbol,
            start_date: REST.format_date(start_date),
            end_date: REST.format_date(end_date),
            interval: interval.to_s,
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetIndexHistoryOhlc, request)
          rows_to_data(response, OHLCRow)
        end

        # Get intraday price ticks
        def history_price(symbol, date:, interval:, start_time: "09:30:00", end_time: "16:00:00")
          request = ::Endpoints::IndexHistoryPriceRequest.new(
            auth_token: auth_token,
            symbol: symbol,
            date: REST.format_date(date),
            interval: interval.to_s,
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetIndexHistoryPrice, request)
          rows_to_data(response, PriceRow)
        end

        # Get price at specific time across date range
        def at_time_price(symbol, start_date:, end_date:, time_of_day:)
          request = ::Endpoints::IndexAtTimePriceRequest.new(
            auth_token: auth_token,
            symbol: symbol,
            start_date: REST.format_date(start_date),
            end_date: REST.format_date(end_date),
            time_of_day: time_of_day,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetIndexAtTimePrice, request)
          rows_to_data(response, PriceRow)
        end

        private

        def connection
          REST.connection
        end

        def auth_token
          ::Endpoints::AuthToken.new(session_uuid: connection.session.session_id)
        end

        def rows_to_data(response, data_class)
          headers = response[:headers].map { |h| h.downcase.to_sym }
          response[:rows].map do |row|
            data_class.new(**headers.zip(row).to_h)
          end
        end
      end
    end
  end
end
