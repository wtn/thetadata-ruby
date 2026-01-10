module ThetaData
  module REST
    module Calendar
      class << self
        # Get today's market schedule
        # @return [CalendarDayRow] today's schedule (type, open, close)
        def today
          request = ::Endpoints::CalendarOpenTodayRequest.new(
            auth_token: auth_token,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetCalendarOpenToday, request)
          rows_to_data(response, CalendarDayRow).first
        end

        # Get market schedule for a specific date
        # @param date [Date, String] the date to check
        # @return [CalendarDayRow] schedule for the date (type, open, close)
        def on_date(date)
          request = ::Endpoints::CalendarOnDateRequest.new(
            auth_token: auth_token,
            date: REST.format_date(date),
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetCalendarOnDate, request)
          rows_to_data(response, CalendarDayRow).first
        end

        # Get all market holidays for a given year
        # @param year [Integer, String] the year to get holidays for
        # @return [Array<CalendarYearRow>] list of holidays (date, type, open, close)
        def year(year)
          request = ::Endpoints::CalendarYearRequest.new(
            auth_token: auth_token,
            year: year.to_s,
            terminal_git_commit: ThetaData::TERMINAL_GIT_COMMIT,
          )

          response = connection.call(:GetCalendarYear, request)
          rows_to_data(response, CalendarYearRow)
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
