import { neon } from '@neondatabase/serverless'

export const sql = neon(process.env.DATABASE_URL!)

export const parseCSV = (v: string | undefined): string[] =>
  v ? v.split(',').filter(Boolean) : []

export const parseCSVNum = (v: string | undefined): number[] =>
  parseCSV(v).map(Number).filter(n => !isNaN(n))

export const isISODate = (v: string | undefined): v is string =>
  typeof v === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(v)
