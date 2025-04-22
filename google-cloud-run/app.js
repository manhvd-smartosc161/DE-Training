const express = require("express");
const app = express();
const port = process.env.PORT || 8080;

app.get("/", (req, res) => {
  res.send("Chào các con giời! Tôi là Mạnh (Ko Pin Mạnh) đây!");
});

app.listen(port, () => {
  console.log(`Server đang chạy tại http://localhost:${port}`);
});
