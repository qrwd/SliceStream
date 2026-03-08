#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use tauri::{WebviewUrl, WebviewWindowBuilder};

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            let _ = WebviewWindowBuilder::new(app, "main", WebviewUrl::App("index.html".into()))
                .title("SliceStream")
                .inner_size(1440.0, 960.0)
                .min_inner_size(1200.0, 760.0)
                .resizable(true)
                .visible(true)
                .build()?;

            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running SliceStream desktop app");
}
