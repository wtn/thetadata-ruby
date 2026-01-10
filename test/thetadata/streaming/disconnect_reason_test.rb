require "thetadata"

describe ThetaData::Streaming::DisconnectReason do
  let(:dr) { ThetaData::Streaming::DisconnectReason }

  with "constants" do
    it "defines ACCOUNT_ALREADY_CONNECTED as 6" do
      expect(dr::ACCOUNT_ALREADY_CONNECTED).to be == 6
    end

    it "defines SERVER_RESTARTING as 15" do
      expect(dr::SERVER_RESTARTING).to be == 15
    end

    it "defines TOO_MANY_REQUESTS as 12" do
      expect(dr::TOO_MANY_REQUESTS).to be == 12
    end
  end

  with ".name" do
    it "returns name for known code" do
      expect(dr.name(6)).to be == "ACCOUNT_ALREADY_CONNECTED"
    end

    it "returns UNKNOWN for unknown code" do
      expect(dr.name(999)).to be == "UNKNOWN(999)"
    end
  end

  with ".reconnectable?" do
    it "returns false for ACCOUNT_ALREADY_CONNECTED" do
      expect(dr.reconnectable?(dr::ACCOUNT_ALREADY_CONNECTED)).to be == false
    end

    it "returns true for SERVER_RESTARTING" do
      expect(dr.reconnectable?(dr::SERVER_RESTARTING)).to be == true
    end

    it "returns true for TIMED_OUT" do
      expect(dr.reconnectable?(dr::TIMED_OUT)).to be == true
    end

    it "returns false for INVALID_CREDENTIALS" do
      expect(dr.reconnectable?(dr::INVALID_CREDENTIALS)).to be == false
    end
  end
end
