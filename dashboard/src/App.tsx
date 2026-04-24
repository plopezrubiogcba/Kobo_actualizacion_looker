import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { NavBar } from '@/components/layout/NavBar'
import { FlashPage } from '@/modules/flash/FlashPage'
import { FlashMapa } from '@/modules/flash/FlashMapa'

function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen flex flex-col bg-gray-50">
        <NavBar />
        <Routes>
          <Route path="/"     element={<FlashPage />} />
          <Route path="/mapa" element={<FlashMapa />} />
        </Routes>
      </div>
    </BrowserRouter>
  )
}

export default App
