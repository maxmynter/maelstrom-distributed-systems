use std::io;

fn main() {
    let mut buffer = String::new();

    println!("Type and the program will answer with what it received.");
    loop {
        let stdin = io::stdin();
        let _ = stdin.read_line(&mut buffer).expect("Failed to read line");
        println!("Received: {}", buffer);
        buffer = String::new();
    }
}
