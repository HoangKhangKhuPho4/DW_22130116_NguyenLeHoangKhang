const API_BASE = "http://localhost:8080/api";

let analystChart = null;  // FIX QUAN TRỌNG

// Load mặc định
loadTopCoins();
loadOverview();

// ------------------ Navigation ------------------
function showPage(id) {
    document.querySelectorAll(".page").forEach(p => p.style.display = "none");
    document.getElementById(id).style.display = "block";
}

function formatNumber(n) {
    return n.toLocaleString("en-US");
}

function formatChange(v) {
    const cls = v >= 0 ? "green" : "red";
    return `<span class="${cls}">${v.toFixed(2)}%</span>`;
}

// ------------------ Top Coins ------------------
async function loadTopCoins() {
    const res = await fetch(`${API_BASE}/top-coins`);
    const data = (await res.json()).data;

    let html = "";
    data.forEach(c => {
        html += `
            <tr>
                <td>${c.MarketCapRank}</td>
                <td>${c.CoinName}</td>
                <td>${c.Symbol.toUpperCase()}</td>
                <td>$${formatNumber(c.Price)}</td>
                <td>$${formatNumber(c.MarketCap)}</td>
                <td>$${formatNumber(c.Volume24h)}</td>
                <td>${formatChange(c.PctChange24h)}</td>
            </tr>
        `;
    });

    document.getElementById("topCoinsTable").innerHTML = html;

    new Chart(document.getElementById("topCoinsChart"), {
        type: "bar",
        data: {
            labels: data.map(c => c.Symbol.toUpperCase()),
            datasets: [{
                label: "Market Cap (USD)",
                data: data.map(c => c.MarketCap),
                backgroundColor: "rgba(30, 64, 175, 0.6)",
                borderColor: "rgb(30, 64, 175)",
                borderWidth: 1
            }]
        }
    });
}

// ------------------ Overview ------------------
async function loadOverview() {
    const res = await fetch(`${API_BASE}/overview`);
    const data = (await res.json()).data;

    new Chart(document.getElementById("overviewChart"), {
        type: "line",
        data: {
            labels: data.map(d => d.DateKey),
            datasets: [{
                label: "Total Market Cap",
                data: data.map(d => d.TotalMarketCap),
                borderColor: "#2563eb",
                borderWidth: 2,
                tension: 0.1
            }]
        }
    });
}

// ------------------ Analyst ------------------
async function loadAnalyst() {
    const symbol = document.getElementById("symbolInput").value.trim();
    if (!symbol) return alert("Nhập symbol trước!");

    const res = await fetch(`${API_BASE}/analyst?symbol=${symbol}`);
    const data = (await res.json()).data;

    // ❗ FIX: Destroy chart cũ nếu tồn tại
    if (analystChart) analystChart.destroy();

    // ❗ Vẽ biểu đồ mới
    analystChart = new Chart(document.getElementById("analystChart"), {
        type: "line",
        data: {
            labels: data.map(d => d.DateKey),
            datasets: [{
                label: `${symbol.toUpperCase()} Price`,
                data: data.map(d => d.Price),
                borderColor: "#16a34a",
                borderWidth: 2,
                tension: 0.1
            }]
        }
    });
}
