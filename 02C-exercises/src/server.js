import express from 'express'
import multer from 'multer'
import { exerciseRoutes } from './api/exerciseRoutes.js'
import { simulationRoutes } from './api/simulationRoutes.js'
import path from 'path'
import { fileURLToPath } from 'url'

// resolve __dirname for ES modules
const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

// load config
const SERVER_PORT = process.env.SERVER_PORT || 5502

// server instance
const app = express()

// multer config
const upload = multer({ storage: multer.memoryStorage() })

// middleware for JSON parsing
app.use(express.json())

// serve static files from 'public' folder
app.use(express.static(path.join(__dirname, 'public')));

// routes
app.use('/exercises', exerciseRoutes(upload))
app.use('/simulations', simulationRoutes())

// start server
app.listen(SERVER_PORT, () => {
  console.log(`EXERCISE MANAGER => server running at ${SERVER_PORT}`)
})