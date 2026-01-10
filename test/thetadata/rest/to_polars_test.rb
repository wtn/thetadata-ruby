require "thetadata"
require "date"

describe ThetaData::REST do
  with ".to_polars" do
    it "converts an array of Data rows into a Polars::DataFrame" do
      rows = [
        ThetaData::REST::OHLCRow.new(
          timestamp: Time.new(2024, 12, 1, 9, 30, 0),
          open: BigDecimal("150.00"),
          high: BigDecimal("151.00"),
          low: BigDecimal("149.50"),
          close: BigDecimal("150.75"),
          volume: 10000,
          count: 50,
          vwap: BigDecimal("150.50"),
        ),
        ThetaData::REST::OHLCRow.new(
          timestamp: Time.new(2024, 12, 1, 9, 31, 0),
          open: BigDecimal("150.75"),
          high: BigDecimal("151.50"),
          low: BigDecimal("150.50"),
          close: BigDecimal("151.25"),
          volume: 8000,
          count: 40,
          vwap: BigDecimal("151.00"),
        ),
      ]

      df = ThetaData::REST.to_polars(rows)

      expect(df).to be_a(Polars::DataFrame)
      expect(df.height).to be == 2
      expect(df.columns).to be == %w[timestamp open high low close volume count vwap]
      expect(df["volume"].to_a).to be == [10000, 8000]
    end

    it "wraps a single Data row into a one-row DataFrame" do
      row = ThetaData::REST::SnapshotPriceRow.new(
        timestamp: Time.new(2024, 12, 2),
        symbol: "SPX",
        price: BigDecimal("5910.25"),
      )

      df = ThetaData::REST.to_polars(row)

      expect(df.height).to be == 1
      expect(df["symbol"].to_a).to be == ["SPX"]
    end

    it "accepts plain hashes (e.g. interest_rate.history_eod output)" do
      rows = [
        {date: Date.new(2024, 12, 1), rate: BigDecimal("4.25")},
        {date: Date.new(2024, 12, 2), rate: BigDecimal("4.26")},
      ]

      df = ThetaData::REST.to_polars(rows)

      expect(df.height).to be == 2
      expect(df.columns).to be == %w[date rate]
    end

    it "returns an empty DataFrame for nil" do
      df = ThetaData::REST.to_polars(nil)

      expect(df).to be_a(Polars::DataFrame)
      expect(df.height).to be == 0
    end

    it "returns an empty DataFrame for an empty array" do
      df = ThetaData::REST.to_polars([])

      expect(df).to be_a(Polars::DataFrame)
      expect(df.height).to be == 0
    end
  end
end
