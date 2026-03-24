def evaluate_training_results(results_df):
    """
    Minimal evaluation helper for WS4.
    """
    if results_df is None:
        return {"status": "no_results"}
    return {"status": "ok"}
