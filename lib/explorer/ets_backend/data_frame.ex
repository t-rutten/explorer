defmodule Explorer.EtsBackend.DataFrame do
  @behaviour Explorer.Backend.DataFrame

  defstruct table: nil

  @impl true
  def from_tabular(data) when is_list(data) do
    table = :ets.new(:explorer_backend, [:ordered_set])

    table_data =
      data
      |> Keyword.values()
      |> Enum.zip()
      |> Enum.with_index()
      |> Enum.map(fn {vals, id} -> {id, vals} end)

    :ets.insert(table, table_data)
    %{data: %__MODULE__{table: table}, name: Keyword.keys(data)}
  end

  @impl true
  def head(%{data: %__MODULE__{table: table}} = df, count) do
    match_spec = [{:"$1", [], [:"$1"]}]
    (fn {1, data} -> data end) |> :ets.fun2ms()
    {data, _continuation} = :ets.select(table, match_spec, count)

    # show which data did we actually selected
    IO.inspect(data)

    table = :ets.new(:explorer_backend, [:ordered_set])
    :ets.insert(table, data)
    %{df | data: %__MODULE__{table: table}}
  end
end
