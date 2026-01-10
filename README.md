# Theta Data

Ruby client for [Theta Data](https://thetadata.net/). Built on [async-grpc](https://github.com/socketry/async-grpc) and [protocol-fpss](https://github.com/wtn/protocol-fpss).

## Usage

Account credentials are read from the environment:

```
THETADATA_ACCOUNT_EMAIL=user@example.com
THETADATA_ACCOUNT_PASSWORD=yourpassword
```

### REST API

```ruby
require "thetadata"

ThetaData::REST::Index.history_eod("SPX", start_date: Date.new(2024, 12, 1), end_date: Date.new(2024, 12, 5))
# => Array of ThetaData::REST::EODRow

ThetaData::REST::Index.snapshot_price("SPX", "VIX")
# => Array of ThetaData::REST::SnapshotPriceRow
```

### Streaming API

```ruby
require "thetadata"

ThetaData.configure do |config|
  config.env = :dev  # replay server
end

ThetaData::Streaming::Index.price_stream("SPX") do |event|
  tick = event[:tick]
  puts "SPX: $#{tick.price_decimal} at #{tick.time}"
end
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/wtn/thetadata-ruby.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
