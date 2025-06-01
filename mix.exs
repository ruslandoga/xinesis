defmodule Kinesis.MixProject do
  use Mix.Project

  def project do
    [
      app: :kinesis,
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
  defp extra_applications(_env), do: []

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:mint, "~> 1.7"},
      {:telemetry, "~> 1.3"}
    ]
  end
end
