export function DependencyTable({ rows }: { rows: any[] }) {
  return (
    <div className="bg-white p-4 rounded-xl shadow">
      <h3 className="font-semibold mb-2">Dependencia China >50%</h3>
      <table className="w-full text-sm">
        <thead><tr><th className="text-left">HS10</th><th className="text-left">Share China</th></tr></thead>
        <tbody>
          {rows.slice(0, 10).map((r) => (
            <tr key={r.hs10} className="border-t">
              <td>{r.hs10}</td>
              <td>{(r.share_china * 100).toFixed(1)}%</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
