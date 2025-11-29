use std::fmt;

pub struct Hex<X>(pub X);

impl<X: AsRef<[u8]>> fmt::Display for Hex<X> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for x in self.0.as_ref().iter() {
            write!(f, "{:02x}", x)?;
        }
        Ok(())
    }
}
