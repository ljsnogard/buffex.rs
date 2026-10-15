pub trait TrConsumerState {
    /// Tell how many units can be read and indicate whether the producer
    /// end is closed.
    fn consumer_state(&self) -> Option<(usize, bool)> {
        Option::None
    }
}

pub trait TrProducerState {
    /// Tell how many units can be written into and indicate whether the
    /// consumer end is closed.
    fn producer_state(&self) -> Option<(usize, bool)> {
        Option::None
    }
}
