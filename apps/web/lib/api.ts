export async function fetchOverview(year?: number) {
  const qs = year ? `?year=${year}` : '';
  const res = await fetch(`http://localhost:8000/api/kpis/overview${qs}`, { cache: 'no-store' });
  if (!res.ok) throw new Error('Error overview');
  return res.json();
}

export async function fetchDependency() {
  const res = await fetch('http://localhost:8000/api/kpis/dependency', { cache: 'no-store' });
  if (!res.ok) throw new Error('Error dependency');
  return res.json();
}
