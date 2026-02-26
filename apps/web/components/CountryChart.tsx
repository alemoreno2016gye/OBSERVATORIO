'use client'

import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'

export function CountryChart({ data }: { data: Array<{country_name: string; fob: number}> }) {
  return (
    <div className="bg-white p-4 rounded-xl shadow h-80">
      <h3 className="font-semibold mb-2">Ranking de países</h3>
      <ResponsiveContainer width="100%" height="90%">
        <BarChart data={data}>
          <XAxis dataKey="country_name" />
          <YAxis />
          <Tooltip />
          <Bar dataKey="fob" fill="#2563eb" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
