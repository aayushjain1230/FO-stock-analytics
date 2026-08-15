function openCommandPalette() {
  const palette = document.getElementById("commandPalette");
  if (palette) palette.classList.add("active");
}

document.addEventListener("keydown", (event) => {
  if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
    event.preventDefault();
    openCommandPalette();
  }
});
