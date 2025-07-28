defmodule Xinesis.MixProject do
  use Mix.Project

  def project do
    [
      app: :xinesis,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger | extra_applications(Mix.env())]
    ]
  end

  defp extra_applications(:test), do: [:inets]
  defp extra_applications(_), do: []

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:mint, "~> 1.7"},
      {:telemetry, "~> 1.0"},
      {:nimble_options, "~> 1.0"},
      {:broadway, "~> 1.2"},
      {:aws, "~> 1.0", optional: true},
      {:ex_aws_dynamo, "~> 4.2", optional: true},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end
end
