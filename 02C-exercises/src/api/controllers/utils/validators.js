export function validateExerciseData({ origin, simulationType, shortDescription }) {
  if (origin && !['CA-SUL', 'CA-LESTE'].includes(origin)) {
    return 'Invalid origin. Use CA-SUL or CA-LESTE';
  }
  if (simulationType && !['viva', 'virtual', 'construtiva'].includes(simulationType)) {
    return 'Invalid simulation type. Use viva, virtual, or construtiva';
  }
  if (shortDescription && !/^[a-zA-Z0-9]+$/.test(shortDescription)) {
    return 'Invalid short description. Use only letters and numbers';
  }
  return null;
}

export function generateTimestamp() {
  const now = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`;
}