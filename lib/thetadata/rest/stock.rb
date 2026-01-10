module ThetaData
  module REST
    module Stock
      class << self
        # List all available stock symbols
        # @return [Array<String>] symbol names
        def list_symbols
          request = ::Endpoints::StockListSymbolsRequest.new(
            query_info: query_info,
          )

          response = connection.call(:GetStockListSymbols, request)
          response[:rows].map(&:first)
        end

        # List available dates for given symbols
        # @return [Array<Date>] available dates
        def list_dates(*symbols, request_type: "QUOTE")
          symbols = symbols.flatten
          request = ::Endpoints::StockListDatesRequest.new(
            query_info: query_info,
            params: ::Endpoints::StockListDatesRequestQuery.new(
              symbol: symbols,
              request_type: request_type,
            ),
          )

          response = connection.call(:GetStockListDates, request)
          response[:rows].map { |row| Date.strptime(row.first, "%Y-%m-%d") }
        end

        # Get current session OHLC
        def snapshot_ohlc(*symbols, venue: nil, min_time: nil)
          symbols = symbols.flatten
          request = ::Endpoints::StockSnapshotOhlcRequest.new(
            query_info: query_info,
            params: ::Endpoints::StockSnapshotOhlcRequestQuery.new(
              symbol: symbols,
              venue: venue,
              min_time: REST.format_time(min_time),
            ),
          )

          response = connection.call(:GetStockSnapshotOhlc, request)
          result = rows_to_data(response, SnapshotOHLCRow)
          symbols.length == 1 ? result.first : result
        end

        # Get current trade snapshot
        def snapshot_trade(*symbols, venue: nil, min_time: nil)
          symbols = symbols.flatten
          request = ::Endpoints::StockSnapshotTradeRequest.new(
            query_info: query_info,
            params: ::Endpoints::StockSnapshotTradeRequestQuery.new(
              symbol: symbols,
              venue: venue,
              min_time: REST.format_time(min_time),
            ),
          )

          response = connection.call(:GetStockSnapshotTrade, request)
          result = rows_to_data(response, SnapshotTradeRow)
          symbols.length == 1 ? result.first : result
        end

        # Get current quote snapshot
        def snapshot_quote(*symbols, venue: nil, min_time: nil)
          symbols = symbols.flatten
          request = ::Endpoints::StockSnapshotQuoteRequest.new(
            query_info: query_info,
            params: ::Endpoints::StockSnapshotQuoteRequestQuery.new(
              symbol: symbols,
              venue: venue,
              min_time: REST.format_time(min_time),
            ),
          )

          response = connection.call(:GetStockSnapshotQuote, request)
          result = rows_to_data(response, SnapshotQuoteRow)
          symbols.length == 1 ? result.first : result
        end

        # Get current market value snapshot
        def snapshot_market_value(*symbols, venue: nil, min_time: nil)
          symbols = symbols.flatten
          request = ::Endpoints::StockSnapshotMarketValueRequest.new(
            query_info: query_info,
            params: ::Endpoints::StockSnapshotMarketValueRequestQuery.new(
              symbol: symbols,
              venue: venue,
              min_time: REST.format_time(min_time),
            ),
          )

          response = connection.call(:GetStockSnapshotMarketValue, request)
          result = rows_to_data(response, SnapshotMarketValueRow)
          symbols.length == 1 ? result.first : result
        end

        # Get end-of-day history
        def history_eod(symbol, start_date:, end_date:)
          request = ::Endpoints::StockHistoryEodRequest.new(
            query_info: query_info,
            params: ::Endpoints::StockHistoryEodRequestQuery.new(
              symbol: symbol,
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
            ),
          )

          response = connection.call(:GetStockHistoryEod, request)
          rows_to_data(response, EODRow)
        end

        # Get intraday OHLC bars. Pass either `date:` for a single day or
        # `start_date:`/`end_date:` for a multi-day range (server caps at 1 month).
        def history_ohlc(symbol, interval:, date: nil, start_date: nil, end_date: nil, start_time: nil, end_time: nil, venue: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::StockHistoryOhlcRequest.new(
            query_info: query_info,
            params: ::Endpoints::StockHistoryOhlcRequestQuery.new(
              symbol: symbol,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              interval: REST.format_interval(interval),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              venue: venue,
            ),
          )

          response = connection.call(:GetStockHistoryOhlc, request)
          rows_to_data(response, OHLCRow)
        end

        # Get historical trades. Pass either `date:` or `start_date:`/`end_date:`.
        def history_trade(symbol, date: nil, start_date: nil, end_date: nil, start_time: nil, end_time: nil, venue: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::StockHistoryTradeRequest.new(
            query_info: query_info,
            params: ::Endpoints::StockHistoryTradeRequestQuery.new(
              symbol: symbol,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              venue: venue,
            ),
          )

          response = connection.call(:GetStockHistoryTrade, request)
          rows_to_data(response, TradeRow)
        end

        # Get historical quotes. Pass either `date:` or `start_date:`/`end_date:`.
        def history_quote(symbol, interval:, date: nil, start_date: nil, end_date: nil, start_time: nil, end_time: nil, venue: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::StockHistoryQuoteRequest.new(
            query_info: query_info,
            params: ::Endpoints::StockHistoryQuoteRequestQuery.new(
              symbol: symbol,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              interval: REST.format_interval(interval),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              venue: venue,
            ),
          )

          response = connection.call(:GetStockHistoryQuote, request)
          rows_to_data(response, QuoteRow)
        end

        # Get historical trades and quotes combined. Pass either `date:` or `start_date:`/`end_date:`.
        def history_trade_quote(symbol, date: nil, start_date: nil, end_date: nil, start_time: nil, end_time: nil, exclusive: nil, venue: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::StockHistoryTradeQuoteRequest.new(
            query_info: query_info,
            params: ::Endpoints::StockHistoryTradeQuoteRequestQuery.new(
              symbol: symbol,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              exclusive: exclusive,
              venue: venue,
            ),
          )

          response = connection.call(:GetStockHistoryTradeQuote, request)
          rows_to_data(response, TradeQuoteRow)
        end

        # Get trade at specific time across date range
        def at_time_trade(symbol, start_date:, end_date:, time_of_day:, venue: nil)
          request = ::Endpoints::StockAtTimeTradeRequest.new(
            query_info: query_info,
            params: ::Endpoints::StockAtTimeTradeRequestQuery.new(
              symbol: symbol,
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              time_of_day: time_of_day,
              venue: venue,
            ),
          )

          response = connection.call(:GetStockAtTimeTrade, request)
          rows_to_data(response, TradeRow)
        end

        # Get quote at specific time across date range
        def at_time_quote(symbol, start_date:, end_date:, time_of_day:, venue: nil)
          request = ::Endpoints::StockAtTimeQuoteRequest.new(
            query_info: query_info,
            params: ::Endpoints::StockAtTimeQuoteRequestQuery.new(
              symbol: symbol,
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              time_of_day: time_of_day,
              venue: venue,
            ),
          )

          response = connection.call(:GetStockAtTimeQuote, request)
          rows_to_data(response, QuoteRow)
        end

        private

        def connection
          REST.connection
        end

        def query_info
          connection.query_info
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
