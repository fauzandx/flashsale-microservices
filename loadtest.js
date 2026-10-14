import http from 'k6/http';
import { check } from 'k6';

export const options = {
  // Simulasi 100 Virtual Users (VUs) yang menembak API secara konstan selama 5 detik
  vus: 2000,
  duration: '15s',
};

export default function () {
  const url = 'http://localhost:8081/api/flashsale/checkout';
  
  // Mengirimkan payload checkout dengan Virtual User ID yang unik
  const payload = JSON.stringify({
    user_id: `user-${__VU}-${__ITER}`,
    item_id: 'item-flash-99',
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);

  // Validasi bahwa respon server berada di rentang 200 (Success) atau 409 (Stock Out)
  check(res, {
    'status is 200 or 409': (r) => r.status === 200 || r.status === 409,
  });
}