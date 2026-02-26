import { fetchDependency, fetchOverview } from '../lib/api'
import { KpiCards } from '../components/KpiCards'
import { CountryChart } from '../components/CountryChart'
import { DependencyTable } from '../components/DependencyTable'

export default async function Home() {
  const [overview, dependency] = await Promise.all([fetchOverview(), fetchDependency()])

  return (
    <main className="p-8 space-y-6">
      <h1 className="text-3xl font-bold">Observatorio Ecuador–China</h1>
      <KpiCards overview={overview} />
      <section className="grid md:grid-cols-2 gap-4">
        <CountryChart data={overview.country_ranking} />
        <DependencyTable rows={dependency} />
      </section>
    </main>
  )
}
