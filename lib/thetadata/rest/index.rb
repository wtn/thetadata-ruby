module ThetaData
  module REST
    module Index
      class << self
        # List all available index symbols
        # @return [Array<String>] symbol names
        def list_symbols
          request = ::Endpoints::IndexListSymbolsRequest.new(
            query_info: query_info,
          )

          response = connection.call(:GetIndexListSymbols, request)
          response[:rows].map(&:first)
        end

        # List available dates for given symbols
        # @return [Array<Date>] available dates
        def list_dates(*symbols)
          symbols = symbols.flatten
          request = ::Endpoints::IndexListDatesRequest.new(
            query_info: query_info,
            params: ::Endpoints::IndexListDatesRequestQuery.new(
              symbol: symbols,
            ),
          )

          response = connection.call(:GetIndexListDates, request)
          response[:rows].map { |row| Date.strptime(row.first, "%Y-%m-%d") }
        end

        # Get current session OHLC for symbols
        def snapshot_ohlc(*symbols, min_time: nil)
          symbols = symbols.flatten
          request = ::Endpoints::IndexSnapshotOhlcRequest.new(
            query_info: query_info,
            params: ::Endpoints::IndexSnapshotOhlcRequestQuery.new(
              symbol: symbols,
              min_time: REST.format_time(min_time),
            ),
          )

          response = connection.call(:GetIndexSnapshotOhlc, request)
          result = rows_to_data(response, SnapshotOHLCRow)
          symbols.length == 1 ? result.first : result
        end

        # Get current price snapshot for symbols
        def snapshot_price(*symbols, min_time: nil)
          symbols = symbols.flatten
          request = ::Endpoints::IndexSnapshotPriceRequest.new(
            query_info: query_info,
            params: ::Endpoints::IndexSnapshotPriceRequestQuery.new(
              symbol: symbols,
              min_time: REST.format_time(min_time),
            ),
          )

          response = connection.call(:GetIndexSnapshotPrice, request)
          result = rows_to_data(response, SnapshotPriceRow)
          symbols.length == 1 ? result.first : result
        end

        # Get current market value snapshot
        def snapshot_market_value(*symbols, min_time: nil)
          symbols = symbols.flatten
          request = ::Endpoints::IndexSnapshotMarketValueRequest.new(
            query_info: query_info,
            params: ::Endpoints::IndexSnapshotMarketValueRequestQuery.new(
              symbol: symbols,
              min_time: REST.format_time(min_time),
            ),
          )

          response = connection.call(:GetIndexSnapshotMarketValue, request)
          result = rows_to_data(response, IndexSnapshotMarketValueRow)
          symbols.length == 1 ? result.first : result
        end

        # Get end-of-day history
        def history_eod(symbol, start_date:, end_date:)
          request = ::Endpoints::IndexHistoryEodRequest.new(
            query_info: query_info,
            params: ::Endpoints::IndexHistoryEodRequestQuery.new(
              symbol: symbol,
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
            ),
          )

          response = connection.call(:GetIndexHistoryEod, request)
          rows_to_data(response, EODRow)
        end

        # Get intraday OHLC bars
        def history_ohlc(symbol, start_date:, end_date:, interval:, start_time: nil, end_time: nil)
          request = ::Endpoints::IndexHistoryOhlcRequest.new(
            query_info: query_info,
            params: ::Endpoints::IndexHistoryOhlcRequestQuery.new(
              symbol: symbol,
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              interval: REST.format_interval(interval),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
            ),
          )

          response = connection.call(:GetIndexHistoryOhlc, request)
          rows_to_data(response, OHLCRow)
        end

        # Get intraday price series. Use interval: 0 (or "tick") for every price change.
        # Pass either `date:` for a single day or `start_date:`/`end_date:` for a multi-day range.
        def history_price(symbol, interval:, date: nil, start_date: nil, end_date: nil, start_time: nil, end_time: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::IndexHistoryPriceRequest.new(
            query_info: query_info,
            params: ::Endpoints::IndexHistoryPriceRequestQuery.new(
              symbol: symbol,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              interval: REST.format_interval(interval),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
            ),
          )

          response = connection.call(:GetIndexHistoryPrice, request)
          rows_to_data(response, PriceRow)
        end

        # Get every price change (tick-level data) for a single date.
        # Convenience wrapper for history_price(interval: 0).
        def history_ticks(symbol, date:, start_time: nil, end_time: nil)
          history_price(symbol, date: date, interval: 0, start_time: start_time, end_time: end_time)
        end

        # Get price at specific time across date range
        def at_time_price(symbol, start_date:, end_date:, time_of_day:)
          request = ::Endpoints::IndexAtTimePriceRequest.new(
            query_info: query_info,
            params: ::Endpoints::IndexAtTimePriceRequestQuery.new(
              symbol: symbol,
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              time_of_day: time_of_day,
            ),
          )

          response = connection.call(:GetIndexAtTimePrice, request)
          rows_to_data(response, TradeRow)
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
