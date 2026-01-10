#!/usr/bin/env ruby

# Basic usage example for ThetaData Ruby client
#
# Usage:
#   bundle exec ruby examples/basic_usage.rb

require_relative "../lib/thetadata"

ThetaData.configure do |config|
  config.env = :dev
end

puts "=" * 60
puts "Theta Data"
puts "Environment: #{ThetaData.configuration.env}"
puts "=" * 60

# REST API

Sync do
  puts "\n--- REST API ---\n\n"

  puts "Fetching SPX history (2026-01-02 to 2026-01-09)..."
  data = ThetaData::REST::Index.history_eod("SPX", start_date: Date.new(2026, 1, 2), end_date: Date.new(2026, 1, 9))

  puts "Got #{data.length} rows:"
  data.each do |row|
    puts "  #{row.last_trade}: O=#{'%.2f' % row.open} H=#{'%.2f' % row.high} L=#{'%.2f' % row.low} C=#{'%.2f' % row.close}"
  end

  puts "\nFetching index snapshot prices..."
  prices = ThetaData::REST::Index.snapshot_price("SPX", "VIX")
  prices.each do |p|
    puts "  #{p.symbol}: #{'%.2f' % p.price}"
  end
end

# Streaming API

puts "\n--- Streaming API ---\n\n"
puts "Subscribing to SPX price stream (10 ticks)..."

count = 0
begin
  ThetaData::Streaming::Index.price_stream("SPX") do |event|
    tick = event[:tick]
    puts "SPX: #{'%.2f' % tick.price_decimal} at #{tick.time}"
    count += 1
    break if count >= 10
  end
rescue ThetaData::TimeoutError, ThetaData::ConnectionError
  puts "No data received - markets may be closed." if count == 0
ensure
  puts "Received #{count} ticks." if count > 0
  ThetaData::Streaming.close
end

puts "\n" + "=" * 60
puts "Done!"
puts "=" * 60
