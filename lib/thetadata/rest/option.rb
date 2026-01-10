module ThetaData
  module REST
    module Option
      class << self
        # List all available option root symbols
        # @return [Array<String>] symbol names
        def list_symbols
          request = ::Endpoints::OptionListSymbolsRequest.new(
            query_info: query_info,
          )

          response = connection.call(:GetOptionListSymbols, request)
          response[:rows].map(&:first)
        end

        # List available expirations for symbols
        def list_expirations(*symbols)
          symbols = symbols.flatten
          request = ::Endpoints::OptionListExpirationsRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionListExpirationsRequestQuery.new(
              symbol: symbols,
            ),
          )

          response = connection.call(:GetOptionListExpirations, request)
          rows_to_data(response, ExpirationRow)
        end

        # List available strikes for symbol and expiration
        def list_strikes(*symbols, expiration:)
          symbols = symbols.flatten
          request = ::Endpoints::OptionListStrikesRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionListStrikesRequestQuery.new(
              symbol: symbols,
              expiration: expiration,
            ),
          )

          response = connection.call(:GetOptionListStrikes, request)
          rows_to_data(response, StrikeRow)
        end

        # List available contracts
        def list_contracts(*symbols, date:, request_type: "QUOTE", max_dte: nil)
          symbols = symbols.flatten
          request = ::Endpoints::OptionListContractsRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionListContractsRequestQuery.new(
              symbol: symbols,
              date: REST.format_date(date),
              request_type: request_type,
              max_dte: max_dte,
            ),
          )

          response = connection.call(:GetOptionListContracts, request)
          rows_to_data(response, ContractRow)
        end

        # Get current session OHLC
        def snapshot_ohlc(symbol:, expiration:, strike: nil, right: nil, max_dte: nil, strike_range: nil, min_time: nil)
          request = ::Endpoints::OptionSnapshotOhlcRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionSnapshotOhlcRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              max_dte: max_dte,
              strike_range: strike_range,
              min_time: REST.format_time(min_time),
            ),
          )

          response = connection.call(:GetOptionSnapshotOhlc, request)
          rows_to_data(response, OptionSnapshotOHLCRow)
        end

        # Get current trade snapshot
        def snapshot_trade(symbol:, expiration:, strike: nil, right: nil, strike_range: nil, min_time: nil)
          request = ::Endpoints::OptionSnapshotTradeRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionSnapshotTradeRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              strike_range: strike_range,
              min_time: REST.format_time(min_time),
            ),
          )

          response = connection.call(:GetOptionSnapshotTrade, request)
          rows_to_data(response, OptionSnapshotTradeRow)
        end

        # Get current quote snapshot
        def snapshot_quote(symbol:, expiration:, strike: nil, right: nil, max_dte: nil, strike_range: nil, min_time: nil)
          request = ::Endpoints::OptionSnapshotQuoteRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionSnapshotQuoteRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              max_dte: max_dte,
              strike_range: strike_range,
              min_time: REST.format_time(min_time),
            ),
          )

          response = connection.call(:GetOptionSnapshotQuote, request)
          rows_to_data(response, OptionSnapshotQuoteRow)
        end

        # Get current open interest snapshot
        def snapshot_open_interest(symbol:, expiration:, strike: nil, right: nil, max_dte: nil, strike_range: nil, min_time: nil)
          request = ::Endpoints::OptionSnapshotOpenInterestRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionSnapshotOpenInterestRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              max_dte: max_dte,
              strike_range: strike_range,
              min_time: REST.format_time(min_time),
            ),
          )

          response = connection.call(:GetOptionSnapshotOpenInterest, request)
          rows_to_data(response, SnapshotOpenInterestRow)
        end

        # Get current market value snapshot
        def snapshot_market_value(symbol:, expiration:, strike: nil, right: nil, max_dte: nil, strike_range: nil, min_time: nil)
          request = ::Endpoints::OptionSnapshotMarketValueRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionSnapshotMarketValueRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              max_dte: max_dte,
              strike_range: strike_range,
              min_time: REST.format_time(min_time),
            ),
          )

          response = connection.call(:GetOptionSnapshotMarketValue, request)
          rows_to_data(response, OptionSnapshotMarketValueRow)
        end

        # Get end-of-day history
        def history_eod(symbol:, expiration:, start_date:, end_date:, strike: nil, right: nil, max_dte: nil, strike_range: nil)
          request = ::Endpoints::OptionHistoryEodRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryEodRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              max_dte: max_dte,
              strike_range: strike_range,
            ),
          )

          response = connection.call(:GetOptionHistoryEod, request)
          rows_to_data(response, OptionEODRow)
        end

        # Get intraday OHLC bars. Pass either `date:` or `start_date:`/`end_date:` for a multi-day range.
        def history_ohlc(symbol:, expiration:, interval:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, start_time: nil, end_time: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryOhlcRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryOhlcRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              interval: REST.format_interval(interval),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              strike_range: strike_range,
            ),
          )

          response = connection.call(:GetOptionHistoryOhlc, request)
          rows_to_data(response, OptionOHLCRow)
        end

        # Get historical trades. Pass either `date:` or `start_date:`/`end_date:`.
        def history_trade(symbol:, expiration:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, start_time: nil, end_time: nil, max_dte: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryTradeRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryTradeRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              max_dte: max_dte,
              strike_range: strike_range,
            ),
          )

          response = connection.call(:GetOptionHistoryTrade, request)
          rows_to_data(response, OptionTradeRow)
        end

        # Get historical quotes. Pass either `date:` or `start_date:`/`end_date:`.
        def history_quote(symbol:, expiration:, interval:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, start_time: nil, end_time: nil, max_dte: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryQuoteRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryQuoteRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              interval: REST.format_interval(interval),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              max_dte: max_dte,
              strike_range: strike_range,
            ),
          )

          response = connection.call(:GetOptionHistoryQuote, request)
          rows_to_data(response, OptionQuoteRow)
        end

        # Get historical trades and quotes combined. Pass either `date:` or `start_date:`/`end_date:`.
        def history_trade_quote(symbol:, expiration:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, start_time: nil, end_time: nil, exclusive: nil, max_dte: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryTradeQuoteRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryTradeQuoteRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              exclusive: exclusive,
              max_dte: max_dte,
              strike_range: strike_range,
            ),
          )

          response = connection.call(:GetOptionHistoryTradeQuote, request)
          rows_to_data(response, OptionTradeQuoteRow)
        end

        # Get historical open interest. Pass either `date:` or `start_date:`/`end_date:`.
        def history_open_interest(symbol:, expiration:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, max_dte: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryOpenInterestRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryOpenInterestRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              max_dte: max_dte,
              strike_range: strike_range,
            ),
          )

          response = connection.call(:GetOptionHistoryOpenInterest, request)
          rows_to_data(response, OptionOpenInterestRow)
        end

        # Get trade at specific time across date range
        def at_time_trade(symbol:, expiration:, start_date:, end_date:, time_of_day:, strike: nil, right: nil, max_dte: nil, strike_range: nil)
          request = ::Endpoints::OptionAtTimeTradeRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionAtTimeTradeRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              time_of_day: time_of_day,
              max_dte: max_dte,
              strike_range: strike_range,
            ),
          )

          response = connection.call(:GetOptionAtTimeTrade, request)
          rows_to_data(response, OptionTradeRow)
        end

        # Get quote at specific time across date range
        def at_time_quote(symbol:, expiration:, start_date:, end_date:, time_of_day:, strike: nil, right: nil, max_dte: nil, strike_range: nil)
          request = ::Endpoints::OptionAtTimeQuoteRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionAtTimeQuoteRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              time_of_day: time_of_day,
              max_dte: max_dte,
              strike_range: strike_range,
            ),
          )

          response = connection.call(:GetOptionAtTimeQuote, request)
          rows_to_data(response, OptionQuoteRow)
        end

        # List available dates for option data
        def list_dates(symbol:, expiration:, request_type: "QUOTE", strike: nil, right: nil)
          request = ::Endpoints::OptionListDatesRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionListDatesRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              request_type: request_type,
            ),
          )

          response = connection.call(:GetOptionListDates, request)
          response[:rows].map { |row| Date.strptime(row.first, "%Y-%m-%d") }
        end

        # Get current implied volatility snapshot
        def snapshot_greeks_implied_volatility(symbol:, expiration:, strike: nil, right: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, stock_price: nil, version: nil, max_dte: nil, strike_range: nil, min_time: nil, use_market_value: nil)
          request = ::Endpoints::OptionSnapshotGreeksImpliedVolatilityRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionSnapshotGreeksImpliedVolatilityRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              **greeks_snapshot_params(annual_dividend, rate_type, rate_value, stock_price, version, max_dte, strike_range, min_time, use_market_value),
            ),
          )

          response = connection.call(:GetOptionSnapshotGreeksImpliedVolatility, request)
          rows_to_data(response, GreeksImpliedVolatilityRow)
        end

        # Get current all greeks snapshot
        def snapshot_greeks_all(symbol:, expiration:, strike: nil, right: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, stock_price: nil, version: nil, max_dte: nil, strike_range: nil, min_time: nil, use_market_value: nil)
          request = ::Endpoints::OptionSnapshotGreeksAllRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionSnapshotGreeksAllRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              **greeks_snapshot_params(annual_dividend, rate_type, rate_value, stock_price, version, max_dte, strike_range, min_time, use_market_value),
            ),
          )

          response = connection.call(:GetOptionSnapshotGreeksAll, request)
          rows_to_data(response, GreeksAllRow)
        end

        # Get current first order greeks snapshot
        def snapshot_greeks_first_order(symbol:, expiration:, strike: nil, right: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, stock_price: nil, version: nil, max_dte: nil, strike_range: nil, min_time: nil, use_market_value: nil)
          request = ::Endpoints::OptionSnapshotGreeksFirstOrderRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionSnapshotGreeksFirstOrderRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              **greeks_snapshot_params(annual_dividend, rate_type, rate_value, stock_price, version, max_dte, strike_range, min_time, use_market_value),
            ),
          )

          response = connection.call(:GetOptionSnapshotGreeksFirstOrder, request)
          rows_to_data(response, GreeksFirstOrderRow)
        end

        # Get current second order greeks snapshot
        def snapshot_greeks_second_order(symbol:, expiration:, strike: nil, right: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, stock_price: nil, version: nil, max_dte: nil, strike_range: nil, min_time: nil, use_market_value: nil)
          request = ::Endpoints::OptionSnapshotGreeksSecondOrderRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionSnapshotGreeksSecondOrderRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              **greeks_snapshot_params(annual_dividend, rate_type, rate_value, stock_price, version, max_dte, strike_range, min_time, use_market_value),
            ),
          )

          response = connection.call(:GetOptionSnapshotGreeksSecondOrder, request)
          rows_to_data(response, GreeksSecondOrderRow)
        end

        # Get current third order greeks snapshot
        def snapshot_greeks_third_order(symbol:, expiration:, strike: nil, right: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, stock_price: nil, version: nil, max_dte: nil, strike_range: nil, min_time: nil, use_market_value: nil)
          request = ::Endpoints::OptionSnapshotGreeksThirdOrderRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionSnapshotGreeksThirdOrderRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              **greeks_snapshot_params(annual_dividend, rate_type, rate_value, stock_price, version, max_dte, strike_range, min_time, use_market_value),
            ),
          )

          response = connection.call(:GetOptionSnapshotGreeksThirdOrder, request)
          rows_to_data(response, GreeksThirdOrderRow)
        end

        # Get historical end-of-day greeks
        def history_greeks_eod(symbol:, expiration:, start_date:, end_date:, strike: nil, right: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, version: nil, underlyer_use_nbbo: nil, max_dte: nil, strike_range: nil)
          request = ::Endpoints::OptionHistoryGreeksEodRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryGreeksEodRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              annual_dividend: annual_dividend,
              rate_type: rate_type,
              rate_value: rate_value,
              version: version,
              underlyer_use_nbbo: underlyer_use_nbbo,
              max_dte: max_dte,
              strike_range: strike_range,
            ),
          )

          response = connection.call(:GetOptionHistoryGreeksEod, request)
          rows_to_data(response, GreeksEODRow)
        end

        # Get historical all greeks. Pass either `date:` or `start_date:`/`end_date:`.
        def history_greeks_all(symbol:, expiration:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, interval: nil, start_time: nil, end_time: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, version: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryGreeksAllRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryGreeksAllRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              interval: interval&.to_s,
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              **greeks_calc_params(annual_dividend, rate_type, rate_value, version, strike_range),
            ),
          )

          response = connection.call(:GetOptionHistoryGreeksAll, request)
          rows_to_data(response, GreeksAllRow)
        end

        # Get historical first order greeks. Pass either `date:` or `start_date:`/`end_date:`.
        def history_greeks_first_order(symbol:, expiration:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, interval: nil, start_time: nil, end_time: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, version: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryGreeksFirstOrderRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryGreeksFirstOrderRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              interval: interval&.to_s,
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              **greeks_calc_params(annual_dividend, rate_type, rate_value, version, strike_range),
            ),
          )

          response = connection.call(:GetOptionHistoryGreeksFirstOrder, request)
          rows_to_data(response, GreeksFirstOrderRow)
        end

        # Get historical second order greeks. Pass either `date:` or `start_date:`/`end_date:`.
        def history_greeks_second_order(symbol:, expiration:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, interval: nil, start_time: nil, end_time: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, version: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryGreeksSecondOrderRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryGreeksSecondOrderRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              interval: interval&.to_s,
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              **greeks_calc_params(annual_dividend, rate_type, rate_value, version, strike_range),
            ),
          )

          response = connection.call(:GetOptionHistoryGreeksSecondOrder, request)
          rows_to_data(response, GreeksSecondOrderRow)
        end

        # Get historical third order greeks. Pass either `date:` or `start_date:`/`end_date:`.
        def history_greeks_third_order(symbol:, expiration:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, interval: nil, start_time: nil, end_time: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, version: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryGreeksThirdOrderRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryGreeksThirdOrderRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              interval: interval&.to_s,
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              **greeks_calc_params(annual_dividend, rate_type, rate_value, version, strike_range),
            ),
          )

          response = connection.call(:GetOptionHistoryGreeksThirdOrder, request)
          rows_to_data(response, GreeksThirdOrderRow)
        end

        # Get historical implied volatility greeks. Pass either `date:` or `start_date:`/`end_date:`.
        def history_greeks_implied_volatility(symbol:, expiration:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, interval: nil, start_time: nil, end_time: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, version: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryGreeksImpliedVolatilityRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryGreeksImpliedVolatilityRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              interval: interval&.to_s,
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              **greeks_calc_params(annual_dividend, rate_type, rate_value, version, strike_range),
            ),
          )

          response = connection.call(:GetOptionHistoryGreeksImpliedVolatility, request)
          rows_to_data(response, GreeksImpliedVolatilityRow)
        end

        # Get historical trade greeks (all orders). Pass either `date:` or `start_date:`/`end_date:`.
        def history_trade_greeks_all(symbol:, expiration:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, start_time: nil, end_time: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, version: nil, max_dte: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryTradeGreeksAllRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryTradeGreeksAllRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              **trade_greeks_params(annual_dividend, rate_type, rate_value, version, max_dte, strike_range),
            ),
          )

          response = connection.call(:GetOptionHistoryTradeGreeksAll, request)
          rows_to_data(response, TradeGreeksAllRow)
        end

        # Get historical trade greeks (first order). Pass either `date:` or `start_date:`/`end_date:`.
        def history_trade_greeks_first_order(symbol:, expiration:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, start_time: nil, end_time: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, version: nil, max_dte: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryTradeGreeksFirstOrderRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryTradeGreeksFirstOrderRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              **trade_greeks_params(annual_dividend, rate_type, rate_value, version, max_dte, strike_range),
            ),
          )

          response = connection.call(:GetOptionHistoryTradeGreeksFirstOrder, request)
          rows_to_data(response, TradeGreeksFirstOrderRow)
        end

        # Get historical trade greeks (second order). Pass either `date:` or `start_date:`/`end_date:`.
        def history_trade_greeks_second_order(symbol:, expiration:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, start_time: nil, end_time: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, version: nil, max_dte: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryTradeGreeksSecondOrderRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryTradeGreeksSecondOrderRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              **trade_greeks_params(annual_dividend, rate_type, rate_value, version, max_dte, strike_range),
            ),
          )

          response = connection.call(:GetOptionHistoryTradeGreeksSecondOrder, request)
          rows_to_data(response, TradeGreeksSecondOrderRow)
        end

        # Get historical trade greeks (third order). Pass either `date:` or `start_date:`/`end_date:`.
        def history_trade_greeks_third_order(symbol:, expiration:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, start_time: nil, end_time: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, version: nil, max_dte: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryTradeGreeksThirdOrderRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryTradeGreeksThirdOrderRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              **trade_greeks_params(annual_dividend, rate_type, rate_value, version, max_dte, strike_range),
            ),
          )

          response = connection.call(:GetOptionHistoryTradeGreeksThirdOrder, request)
          rows_to_data(response, TradeGreeksThirdOrderRow)
        end

        # Get historical trade greeks (implied volatility). Pass either `date:` or `start_date:`/`end_date:`.
        def history_trade_greeks_implied_volatility(symbol:, expiration:, date: nil, start_date: nil, end_date: nil, strike: nil, right: nil, start_time: nil, end_time: nil, annual_dividend: nil, rate_type: nil, rate_value: nil, version: nil, max_dte: nil, strike_range: nil)
          REST.validate_date_range!(date, start_date, end_date)
          request = ::Endpoints::OptionHistoryTradeGreeksImpliedVolatilityRequest.new(
            query_info: query_info,
            params: ::Endpoints::OptionHistoryTradeGreeksImpliedVolatilityRequestQuery.new(
              contract_spec: contract_spec(symbol, expiration, strike, right),
              expiration: expiration,
              date: REST.format_date(date),
              start_date: REST.format_date(start_date),
              end_date: REST.format_date(end_date),
              start_time: REST.format_time(start_time),
              end_time: REST.format_time(end_time),
              **trade_greeks_params(annual_dividend, rate_type, rate_value, version, max_dte, strike_range),
            ),
          )

          response = connection.call(:GetOptionHistoryTradeGreeksImpliedVolatility, request)
          rows_to_data(response, TradeGreeksImpliedVolatilityRow)
        end

        private

        def connection
          REST.connection
        end

        def query_info
          connection.query_info
        end

        # Server expects "CALL"/"PUT"; accept the common shorthands and normalize.
        RIGHT_NORMALIZE = {
          nil => nil,
          "CALL" => "CALL", "PUT" => "PUT",
          "call" => "CALL", "put" => "PUT",
          "C" => "CALL", "P" => "PUT",
          "c" => "CALL", "p" => "PUT",
          :call => "CALL", :put => "PUT",
          :CALL => "CALL", :PUT => "PUT",
          :c => "CALL", :p => "PUT",
        }.freeze

        def contract_spec(symbol, expiration, strike, right)
          unless RIGHT_NORMALIZE.key?(right)
            raise ArgumentError, "right must be one of :call, :put, \"CALL\", \"PUT\", \"C\", \"P\" (or nil); got #{right.inspect}"
          end

          ::Endpoints::ContractSpec.new(
            symbol: symbol,
            expiration: expiration,
            strike: strike&.to_s,
            right: RIGHT_NORMALIZE.fetch(right),
          )
        end

        # Shared optional fields for snapshot greeks endpoints.
        def greeks_snapshot_params(annual_dividend, rate_type, rate_value, stock_price, version, max_dte, strike_range, min_time, use_market_value)
          {
            annual_dividend: annual_dividend,
            rate_type: rate_type,
            rate_value: rate_value,
            stock_price: stock_price,
            version: version,
            max_dte: max_dte,
            strike_range: strike_range,
            min_time: REST.format_time(min_time),
            use_market_value: use_market_value,
          }
        end

        # Shared optional fields for intraday history greeks endpoints (interval-based, no max_dte).
        def greeks_calc_params(annual_dividend, rate_type, rate_value, version, strike_range)
          {
            annual_dividend: annual_dividend,
            rate_type: rate_type,
            rate_value: rate_value,
            version: version,
            strike_range: strike_range,
          }
        end

        # Shared optional fields for trade-greeks endpoints (no interval, includes max_dte).
        def trade_greeks_params(annual_dividend, rate_type, rate_value, version, max_dte, strike_range)
          {
            annual_dividend: annual_dividend,
            rate_type: rate_type,
            rate_value: rate_value,
            version: version,
            max_dte: max_dte,
            strike_range: strike_range,
          }
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
