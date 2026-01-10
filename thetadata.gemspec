require_relative "lib/thetadata/version"

Gem::Specification.new do |spec|
  spec.name = "thetadata"
  spec.version = ThetaData::VERSION
  spec.authors = ["William T. Nelson"]
  spec.email = ["35801+wtn@users.noreply.github.com"]

  spec.summary = "Theta Data API client for Ruby"
  spec.homepage = "https://github.com/wtn/thetadata-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.3.0"

  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ Gemfile .gitignore test/])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "bigdecimal"
  spec.add_dependency "protocol-fpss"
  spec.add_dependency "async-grpc"
  spec.add_dependency "zstd-ruby", "~> 2.0"
  spec.add_dependency "polars-df"
  spec.add_dependency "tzinfo", "~> 2.0"
end
