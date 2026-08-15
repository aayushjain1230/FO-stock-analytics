function explainPrediction(payload) {
  return {
    title: payload.prediction_name,
    confidence: payload.confidence,
    drivers: payload.top_positive_drivers || [],
    risks: payload.top_negative_drivers || []
  };
}
