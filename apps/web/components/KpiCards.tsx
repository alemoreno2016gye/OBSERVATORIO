export function KpiCards({ overview }: { overview: any }) {
  const cards = [
    { label: 'Exportaciones FOB', value: overview.total_exports_fob },
    { label: 'Importaciones FOB', value: overview.total_imports_fob },
    { label: 'Balanza Comercial', value: overview.trade_balance },
    { label: 'Costo Logístico', value: overview.logistics_cost },
  ];

  return (
    <div className="grid md:grid-cols-4 gap-4">
      {cards.map((c) => (
        <div key={c.label} className="bg-white rounded-xl shadow p-4">
          <p className="text-sm text-slate-500">{c.label}</p>
          <p className="text-2xl font-semibold">{Number(c.value).toLocaleString()}</p>
        </div>
      ))}
    </div>
  );
}
