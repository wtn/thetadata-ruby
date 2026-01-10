require "thetadata"

describe ThetaData::REST do
  with ".format_interval" do
    it "converts millisecond integers to shorthand" do
      expect(ThetaData::REST.format_interval(100)).to be == "100ms"
      expect(ThetaData::REST.format_interval(500)).to be == "500ms"
      expect(ThetaData::REST.format_interval(1_000)).to be == "1s"
      expect(ThetaData::REST.format_interval(5_000)).to be == "5s"
      expect(ThetaData::REST.format_interval(10_000)).to be == "10s"
      expect(ThetaData::REST.format_interval(15_000)).to be == "15s"
      expect(ThetaData::REST.format_interval(30_000)).to be == "30s"
      expect(ThetaData::REST.format_interval(60_000)).to be == "1m"
      expect(ThetaData::REST.format_interval(300_000)).to be == "5m"
      expect(ThetaData::REST.format_interval(600_000)).to be == "10m"
      expect(ThetaData::REST.format_interval(900_000)).to be == "15m"
      expect(ThetaData::REST.format_interval(1_800_000)).to be == "30m"
      expect(ThetaData::REST.format_interval(3_600_000)).to be == "1h"
    end

    it "passes through shorthand strings unchanged" do
      expect(ThetaData::REST.format_interval("1s")).to be == "1s"
      expect(ThetaData::REST.format_interval("15m")).to be == "15m"
      expect(ThetaData::REST.format_interval("1h")).to be == "1h"
      expect(ThetaData::REST.format_interval("100ms")).to be == "100ms"
    end

    it "converts 0 to tick for every-change data" do
      expect(ThetaData::REST.format_interval(0)).to be == "tick"
    end

    it "raises ArgumentError for unsupported millisecond values" do
      expect { ThetaData::REST.format_interval(999) }.to raise_exception(ArgumentError)
      expect { ThetaData::REST.format_interval(2000) }.to raise_exception(ArgumentError)
    end
  end

  with ".format_time" do
    it "appends .000 to times without milliseconds" do
      expect(ThetaData::REST.format_time("09:30:00")).to be == "09:30:00.000"
      expect(ThetaData::REST.format_time("16:00:00")).to be == "16:00:00.000"
    end

    it "passes through times that already have milliseconds" do
      expect(ThetaData::REST.format_time("09:30:00.000")).to be == "09:30:00.000"
      expect(ThetaData::REST.format_time("09:30:00.500")).to be == "09:30:00.500"
    end

    it "returns nil for nil" do
      expect(ThetaData::REST.format_time(nil)).to be == nil
    end
  end

  with ".format_date" do
    it "formats Date to YYYY-MM-DD string (matches Python client wire format)" do
      expect(ThetaData::REST.format_date(Date.new(2026, 4, 10))).to be == "2026-04-10"
      expect(ThetaData::REST.format_date(Date.new(2024, 1, 2))).to be == "2024-01-02"
    end

    it "returns nil for nil" do
      expect(ThetaData::REST.format_date(nil)).to be == nil
    end

    it "raises ArgumentError for non-Date" do
      expect { ThetaData::REST.format_date("2026-04-10") }.to raise_exception(ArgumentError)
      expect { ThetaData::REST.format_date(20260410) }.to raise_exception(ArgumentError)
    end
  end

  with ".validate_date_range!" do
    let(:d1) { Date.new(2024, 12, 1) }
    let(:d2) { Date.new(2024, 12, 31) }

    it "accepts only date" do
      expect { ThetaData::REST.validate_date_range!(d1, nil, nil) }.not.to raise_exception
    end

    it "accepts only start_date and end_date" do
      expect { ThetaData::REST.validate_date_range!(nil, d1, d2) }.not.to raise_exception
    end

    it "accepts all-nil (caller will surface a server-side error if required)" do
      expect { ThetaData::REST.validate_date_range!(nil, nil, nil) }.not.to raise_exception
    end

    it "raises when both date and start_date are given" do
      expect do
        ThetaData::REST.validate_date_range!(d1, d1, d2)
      end.to raise_exception(ArgumentError, message: be =~ /not both/)
    end

    it "raises when both date and end_date are given" do
      expect do
        ThetaData::REST.validate_date_range!(d1, nil, d2)
      end.to raise_exception(ArgumentError, message: be =~ /not both/)
    end

    it "raises when only start_date is given" do
      expect do
        ThetaData::REST.validate_date_range!(nil, d1, nil)
      end.to raise_exception(ArgumentError, message: be =~ /together/)
    end

    it "raises when only end_date is given" do
      expect do
        ThetaData::REST.validate_date_range!(nil, nil, d2)
      end.to raise_exception(ArgumentError, message: be =~ /together/)
    end
  end
end
