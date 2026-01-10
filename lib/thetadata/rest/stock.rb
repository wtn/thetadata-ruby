module ThetaData
  module REST
    module Stock
      class << self
        # List all available stock symbols
        # @return [Array<String>] symbol names
        def list_symbols
          request = ::Endpoints::StockListSymbolsRequest.new(
            auth_token: auth_token,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetStockListSymbols, request)
          response[:rows].map(&:first)
        end

        # List available dates for given symbols
        # @return [Array<Date>] available dates
        def list_dates(*symbols, request_type: "QUOTE")
          symbols = symbols.flatten
          request = ::Endpoints::StockListDatesRequest.new(
            auth_token: auth_token,
            symbol: symbols,
            request_type: request_type,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetStockListDates, request)
          response[:rows].map { |row| Date.strptime(row.first, "%Y-%m-%d") }
        end

        # Get current session OHLC
        def snapshot_ohlc(*symbols, venue: nil)
          symbols = symbols.flatten
          request = ::Endpoints::StockSnapshotOhlcRequest.new(
            auth_token: auth_token,
            symbol: symbols,
            venue: venue,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetStockSnapshotOhlc, request)
          result = rows_to_data(response, SnapshotOHLCRow)
          symbols.length == 1 ? result.first : result
        end

        # Get current trade snapshot
        def snapshot_trade(*symbols, venue: nil)
          symbols = symbols.flatten
          request = ::Endpoints::StockSnapshotTradeRequest.new(
            auth_token: auth_token,
            symbol: symbols,
            venue: venue,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetStockSnapshotTrade, request)
          result = rows_to_data(response, SnapshotTradeRow)
          symbols.length == 1 ? result.first : result
        end

        # Get current quote snapshot
        def snapshot_quote(*symbols, venue: nil)
          symbols = symbols.flatten
          request = ::Endpoints::StockSnapshotQuoteRequest.new(
            auth_token: auth_token,
            symbol: symbols,
            venue: venue,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetStockSnapshotQuote, request)
          result = rows_to_data(response, SnapshotQuoteRow)
          symbols.length == 1 ? result.first : result
        end

        def snapshot_market_value(*)
          raise NotImplementedError
        end

        # Get end-of-day history
        def history_eod(symbol, start_date:, end_date:)
          request = ::Endpoints::StockHistoryEodRequest.new(
            auth_token: auth_token,
            symbol: symbol,
            start_date: REST.format_date(start_date),
            end_date: REST.format_date(end_date),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetStockHistoryEod, request)
          rows_to_data(response, EODRow)
        end

        # Get intraday OHLC bars
        def history_ohlc(symbol, date:, interval:, start_time: nil, end_time: nil, venue: nil)
          request = ::Endpoints::StockHistoryOhlcRequest.new(
            auth_token: auth_token,
            symbol: symbol,
            date: REST.format_date(date),
            interval: interval.to_s,
            start_time: start_time,
            end_time: end_time,
            venue: venue,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetStockHistoryOhlc, request)
          rows_to_data(response, OHLCRow)
        end

        # Get historical trades
        def history_trade(symbol, date:, start_time: nil, end_time: nil, venue: nil)
          request = ::Endpoints::StockHistoryTradeRequest.new(
            auth_token: auth_token,
            symbol: symbol,
            date: REST.format_date(date),
            start_time: start_time,
            end_time: end_time,
            venue: venue,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetStockHistoryTrade, request)
          rows_to_data(response, TradeRow)
        end

        # Get historical quotes
        def history_quote(symbol, date:, interval:, start_time: nil, end_time: nil, venue: nil)
          request = ::Endpoints::StockHistoryQuoteRequest.new(
            auth_token: auth_token,
            symbol: symbol,
            date: REST.format_date(date),
            interval: interval.to_s,
            start_time: start_time,
            end_time: end_time,
            venue: venue,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetStockHistoryQuote, request)
          rows_to_data(response, QuoteRow)
        end

        # Get historical trades and quotes combined
        def history_trade_quote(symbol, date:, start_time: nil, end_time: nil, exclusive: nil, venue: nil)
          request = ::Endpoints::StockHistoryTradeQuoteRequest.new(
            auth_token: auth_token,
            symbol: symbol,
            date: REST.format_date(date),
            start_time: start_time,
            end_time: end_time,
            exclusive: exclusive,
            venue: venue,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetStockHistoryTradeQuote, request)
          rows_to_data(response, TradeQuoteRow)
        end

        # Get trade at specific time across date range
        def at_time_trade(symbol, start_date:, end_date:, time_of_day:, venue: nil)
          request = ::Endpoints::StockAtTimeTradeRequest.new(
            auth_token: auth_token,
            symbol: symbol,
            start_date: REST.format_date(start_date),
            end_date: REST.format_date(end_date),
            time_of_day: time_of_day,
            venue: venue,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetStockAtTimeTrade, request)
          rows_to_data(response, TradeRow)
        end

        # Get quote at specific time across date range
        def at_time_quote(symbol, start_date:, end_date:, time_of_day:, venue: nil)
          request = ::Endpoints::StockAtTimeQuoteRequest.new(
            auth_token: auth_token,
            symbol: symbol,
            start_date: REST.format_date(start_date),
            end_date: REST.format_date(end_date),
            time_of_day: time_of_day,
            venue: venue,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetStockAtTimeQuote, request)
          rows_to_data(response, QuoteRow)
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
