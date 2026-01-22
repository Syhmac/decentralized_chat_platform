use std::process::Command;

fn main() {
    let com = if cfg!(target_os = "windows") {
        Command::new("cmd")
            .args(["/C", "cd client && npm run tauri dev"])
            .output()
            .expect("failed to execute process")
    } else {
        Command::new("sh")
            .arg("-c")
            .arg("cd client && npm run tauri dev")
            .output()
            .expect("failed to execute process")
    };

    println!("status: {}", com.status);
}
