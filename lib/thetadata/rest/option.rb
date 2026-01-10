module ThetaData
  module REST
    module Option
      class << self
        # List all available option root symbols
        # @return [Array<String>] symbol names
        def list_symbols
          request = ::Endpoints::OptionListSymbolsRequest.new(
            auth_token: auth_token,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionListSymbols, request)
          response[:rows].map(&:first)
        end

        # List available expirations for symbols
        def list_expirations(*symbols)
          symbols = symbols.flatten
          request = ::Endpoints::OptionListExpirationsRequest.new(
            auth_token: auth_token,
            symbol: symbols,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionListExpirations, request)
          rows_to_data(response, ExpirationRow)
        end

        # List available strikes for symbol and expiration
        def list_strikes(*symbols, expiration:)
          symbols = symbols.flatten
          request = ::Endpoints::OptionListStrikesRequest.new(
            auth_token: auth_token,
            symbol: symbols,
            expiration: expiration,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionListStrikes, request)
          rows_to_data(response, StrikeRow)
        end

        # List available contracts
        def list_contracts(*symbols, date:, request_type: "QUOTE")
          symbols = symbols.flatten
          request = ::Endpoints::OptionListContractsRequest.new(
            auth_token: auth_token,
            symbol: symbols,
            date: REST.format_date(date),
            request_type: request_type,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionListContracts, request)
          rows_to_data(response, ContractRow)
        end

        # Get current session OHLC
        def snapshot_ohlc(symbol:, expiration:, strike: nil, right: nil)
          request = ::Endpoints::OptionSnapshotOhlcRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionSnapshotOhlc, request)
          rows_to_data(response, OptionSnapshotOHLCRow)
        end

        # Get current trade snapshot
        def snapshot_trade(symbol:, expiration:, strike: nil, right: nil)
          request = ::Endpoints::OptionSnapshotTradeRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionSnapshotTrade, request)
          rows_to_data(response, OptionSnapshotTradeRow)
        end

        # Get current quote snapshot
        def snapshot_quote(symbol:, expiration:, strike: nil, right: nil)
          request = ::Endpoints::OptionSnapshotQuoteRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionSnapshotQuote, request)
          rows_to_data(response, OptionSnapshotQuoteRow)
        end

        # Get current open interest snapshot
        def snapshot_open_interest(symbol:, expiration:, strike: nil, right: nil)
          request = ::Endpoints::OptionSnapshotOpenInterestRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionSnapshotOpenInterest, request)
          rows_to_data(response, SnapshotOpenInterestRow)
        end

        def snapshot_market_value(*)
          raise NotImplementedError
        end

        # Get end-of-day history
        def history_eod(symbol:, expiration:, start_date:, end_date:, strike: nil, right: nil)
          request = ::Endpoints::OptionHistoryEodRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            start_date: REST.format_date(start_date),
            end_date: REST.format_date(end_date),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryEod, request)
          rows_to_data(response, OptionEODRow)
        end

        # Get intraday OHLC bars
        def history_ohlc(symbol:, expiration:, date:, interval:, strike: nil, right: nil, start_time: nil, end_time: nil)
          request = ::Endpoints::OptionHistoryOhlcRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            interval: interval.to_s,
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryOhlc, request)
          rows_to_data(response, OptionOHLCRow)
        end

        # Get historical trades
        def history_trade(symbol:, expiration:, date:, strike: nil, right: nil, start_time: nil, end_time: nil)
          request = ::Endpoints::OptionHistoryTradeRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryTrade, request)
          rows_to_data(response, OptionTradeRow)
        end

        # Get historical quotes
        def history_quote(symbol:, expiration:, date:, interval:, strike: nil, right: nil, start_time: nil, end_time: nil)
          request = ::Endpoints::OptionHistoryQuoteRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            interval: interval.to_s,
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryQuote, request)
          rows_to_data(response, OptionQuoteRow)
        end

        # Get historical trades and quotes combined
        def history_trade_quote(symbol:, expiration:, date:, strike: nil, right: nil, start_time: nil, end_time: nil, exclusive: nil)
          request = ::Endpoints::OptionHistoryTradeQuoteRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            start_time: start_time,
            end_time: end_time,
            exclusive: exclusive,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryTradeQuote, request)
          rows_to_data(response, OptionTradeQuoteRow)
        end

        # Get historical open interest
        def history_open_interest(symbol:, expiration:, date:, strike: nil, right: nil)
          request = ::Endpoints::OptionHistoryOpenInterestRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryOpenInterest, request)
          rows_to_data(response, OptionOpenInterestRow)
        end

        # Get trade at specific time across date range
        def at_time_trade(symbol:, expiration:, start_date:, end_date:, time_of_day:, strike: nil, right: nil)
          request = ::Endpoints::OptionAtTimeTradeRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            start_date: REST.format_date(start_date),
            end_date: REST.format_date(end_date),
            time_of_day: time_of_day,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionAtTimeTrade, request)
          rows_to_data(response, OptionTradeRow)
        end

        # Get quote at specific time across date range
        def at_time_quote(symbol:, expiration:, start_date:, end_date:, time_of_day:, strike: nil, right: nil)
          request = ::Endpoints::OptionAtTimeQuoteRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            start_date: REST.format_date(start_date),
            end_date: REST.format_date(end_date),
            time_of_day: time_of_day,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionAtTimeQuote, request)
          rows_to_data(response, OptionQuoteRow)
        end

        # List available dates for option data
        def list_dates(symbol:, expiration:, request_type: "QUOTE", strike: nil, right: nil)
          request = ::Endpoints::OptionListDatesRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            request_type: request_type,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionListDates, request)
          response[:rows].map { |row| Date.strptime(row.first, "%Y-%m-%d") }
        end

        # Get current implied volatility snapshot
        def snapshot_greeks_implied_volatility(symbol:, expiration:, strike: nil, right: nil)
          request = ::Endpoints::OptionSnapshotGreeksImpliedVolatilityRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionSnapshotGreeksImpliedVolatility, request)
          rows_to_data(response, GreeksImpliedVolatilityRow)
        end

        # Get current all greeks snapshot
        def snapshot_greeks_all(symbol:, expiration:, strike: nil, right: nil)
          request = ::Endpoints::OptionSnapshotGreeksAllRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionSnapshotGreeksAll, request)
          rows_to_data(response, GreeksAllRow)
        end

        # Get current first order greeks snapshot
        def snapshot_greeks_first_order(symbol:, expiration:, strike: nil, right: nil)
          request = ::Endpoints::OptionSnapshotGreeksFirstOrderRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionSnapshotGreeksFirstOrder, request)
          rows_to_data(response, GreeksFirstOrderRow)
        end

        # Get current second order greeks snapshot
        def snapshot_greeks_second_order(symbol:, expiration:, strike: nil, right: nil)
          request = ::Endpoints::OptionSnapshotGreeksSecondOrderRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionSnapshotGreeksSecondOrder, request)
          rows_to_data(response, GreeksSecondOrderRow)
        end

        # Get current third order greeks snapshot
        def snapshot_greeks_third_order(symbol:, expiration:, strike: nil, right: nil)
          request = ::Endpoints::OptionSnapshotGreeksThirdOrderRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionSnapshotGreeksThirdOrder, request)
          rows_to_data(response, GreeksThirdOrderRow)
        end

        # Get historical end-of-day greeks
        def history_greeks_eod(symbol:, expiration:, start_date:, end_date:, strike: nil, right: nil)
          request = ::Endpoints::OptionHistoryGreeksEodRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            start_date: REST.format_date(start_date),
            end_date: REST.format_date(end_date),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryGreeksEod, request)
          rows_to_data(response, GreeksEODRow)
        end

        # Get historical all greeks
        def history_greeks_all(symbol:, expiration:, date:, strike: nil, right: nil, interval: nil, start_time: nil, end_time: nil)
          request = ::Endpoints::OptionHistoryGreeksAllRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            interval: interval&.to_s,
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryGreeksAll, request)
          rows_to_data(response, GreeksAllRow)
        end

        # Get historical first order greeks
        def history_greeks_first_order(symbol:, expiration:, date:, strike: nil, right: nil, interval: nil, start_time: nil, end_time: nil)
          request = ::Endpoints::OptionHistoryGreeksFirstOrderRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            interval: interval&.to_s,
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryGreeksFirstOrder, request)
          rows_to_data(response, GreeksFirstOrderRow)
        end

        # Get historical second order greeks
        def history_greeks_second_order(symbol:, expiration:, date:, strike: nil, right: nil, interval: nil, start_time: nil, end_time: nil)
          request = ::Endpoints::OptionHistoryGreeksSecondOrderRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            interval: interval&.to_s,
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryGreeksSecondOrder, request)
          rows_to_data(response, GreeksSecondOrderRow)
        end

        # Get historical third order greeks
        def history_greeks_third_order(symbol:, expiration:, date:, strike: nil, right: nil, interval: nil, start_time: nil, end_time: nil)
          request = ::Endpoints::OptionHistoryGreeksThirdOrderRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            interval: interval&.to_s,
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryGreeksThirdOrder, request)
          rows_to_data(response, GreeksThirdOrderRow)
        end

        # Get historical implied volatility greeks
        def history_greeks_implied_volatility(symbol:, expiration:, date:, strike: nil, right: nil, interval: nil, start_time: nil, end_time: nil)
          request = ::Endpoints::OptionHistoryGreeksImpliedVolatilityRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            interval: interval&.to_s,
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryGreeksImpliedVolatility, request)
          rows_to_data(response, GreeksImpliedVolatilityRow)
        end

        # Get historical trade greeks (all orders)
        def history_trade_greeks_all(symbol:, expiration:, date:, strike: nil, right: nil, start_time: nil, end_time: nil)
          request = ::Endpoints::OptionHistoryTradeGreeksAllRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryTradeGreeksAll, request)
          rows_to_data(response, TradeGreeksAllRow)
        end

        # Get historical trade greeks (first order)
        def history_trade_greeks_first_order(symbol:, expiration:, date:, strike: nil, right: nil, start_time: nil, end_time: nil)
          request = ::Endpoints::OptionHistoryTradeGreeksFirstOrderRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryTradeGreeksFirstOrder, request)
          rows_to_data(response, TradeGreeksFirstOrderRow)
        end

        # Get historical trade greeks (second order)
        def history_trade_greeks_second_order(symbol:, expiration:, date:, strike: nil, right: nil, start_time: nil, end_time: nil)
          request = ::Endpoints::OptionHistoryTradeGreeksSecondOrderRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryTradeGreeksSecondOrder, request)
          rows_to_data(response, TradeGreeksSecondOrderRow)
        end

        # Get historical trade greeks (third order)
        def history_trade_greeks_third_order(symbol:, expiration:, date:, strike: nil, right: nil, start_time: nil, end_time: nil)
          request = ::Endpoints::OptionHistoryTradeGreeksThirdOrderRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryTradeGreeksThirdOrder, request)
          rows_to_data(response, TradeGreeksThirdOrderRow)
        end

        # Get historical trade greeks (implied volatility)
        def history_trade_greeks_implied_volatility(symbol:, expiration:, date:, strike: nil, right: nil, start_time: nil, end_time: nil)
          request = ::Endpoints::OptionHistoryTradeGreeksImpliedVolatilityRequest.new(
            auth_token: auth_token,
            contract_spec: contract_spec(symbol, expiration, strike, right),
            date: REST.format_date(date),
            start_time: start_time,
            end_time: end_time,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetOptionHistoryTradeGreeksImpliedVolatility, request)
          rows_to_data(response, TradeGreeksImpliedVolatilityRow)
        end

        private

        def connection
          REST.connection
        end

        def auth_token
          ::Endpoints::AuthToken.new(session_uuid: connection.session.session_id)
        end

        def contract_spec(symbol, expiration, strike, right)
          ::Endpoints::ContractSpec.new(
            symbol: symbol,
            expiration: expiration,
            strike: strike&.to_s,
            right: right&.to_s&.upcase,
          )
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
