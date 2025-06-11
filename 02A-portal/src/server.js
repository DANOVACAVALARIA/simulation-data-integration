const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = 5500;
const PUBLIC_DIR = path.join(__dirname, 'public');

http.createServer((req, res) => {
  const filePath = path.join(PUBLIC_DIR, req.url === '/' ? 'index.html' : req.url);

  fs.readFile(filePath, (err, content) => {
    if (err) {
      res.writeHead(err.code === 'ENOENT' ? 404 : 500, { 'Content-Type': 'text/plain' });
      res.end(err.code === 'ENOENT' ? '404 Not Found' : '500 Server Error');
    } else {
      res.writeHead(200);
      res.end(content);
    }
  });
}).listen(PORT, () => console.log(`Server running at http://localhost:${PORT}`));
