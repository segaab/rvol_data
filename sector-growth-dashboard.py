<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Sector Wave Dashboard MVP</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
  <style>
    body { font-family: 'Inter', sans-serif; margin:0; padding:0; background:#f9fafb; color:#1a1a1a; }
    header { background:#1f77b4; color:white; padding:2rem 1rem; text-align:center; }
    header h1 { margin:0; font-size:2.5rem; }
    header p { font-size:1.2rem; margin-top:.5rem; }
    section { max-width:900px; margin:2rem auto; padding:0 1rem; }
    h2 { color:#1f77b4; margin-top:2rem; }
    p, ul { line-height:1.6; margin-top:.5rem; }
    .cta { text-align:center; margin:2rem 0; }
    .cta a, .pay-button { background:#ff7f0e; color:white; padding:1rem 2rem; font-weight:600; border-radius:6px; text-decoration:none; transition:.3s; display:inline-block; margin:0.5rem; cursor:pointer; }
    .cta a:hover, .pay-button:hover { background:#e67000; }
    footer { background:#1f77b4; color:white; text-align:center; padding:1rem; margin-top:2rem; }
  </style>
</head>
<body>

<header>
  <h1>Sector Wave Dashboard MVP</h1>
  <p>Detect sector growth early, monitor leader-follower dynamics, and ride market waves with confidence.</p>
</header>

<section>
  <h2>Membership Checkout</h2>
  <p>Select your preferred method and complete your membership payment securely via Alchemy Pay.</p>
  <div class="cta">
    <button class="pay-button" onclick="redirectToAlchemyPay()">💳 Pay with Alchemy Pay</button>
  </div>

  <div class="cta">
    <a href="https://t.me/segaab120" target="_blank">Join the Beta / Sign Up via Telegram</a>
  </div>
</section>

<footer>
  &copy; 2025 Sector Wave Dashboard MVP | Contact: @segaab120
</footer>

<script>
  // Simulate fetching a payUrl from your backend API
  async function getAlchemyPayUrl() {
    // Example: normally, you’d call your server here to create a payment order
    // fetch("/create-payment-order") -> returns { payUrl: "https://pay.alchemypay.org/checkout?..." }

    // For demo purposes, we hardcode a mock URL
    return "https://pay.alchemypay.org/checkout?merchantOrderNo=ORDER12345&amount=25&fiat=USD&redirectUrl=https://shrukmonitor.streamlit.app";
  }

  async function redirectToAlchemyPay() {
    const payUrl = await getAlchemyPayUrl();
    window.location.href = payUrl;
  }
</script>

</body>
</html>
