/** @type {import('tailwindcss').Config} */
module.exports = {
    content: ["../views/**/*.html", "./src/**/*.{js,ts,jsx,tsx}"],
    theme: {
        extend: {
            fontFamily: {
                sans: ['Outfit', 'sans-serif'],
            },
            colors: {
                primary: '#4F46E5', // Indigo 600
                secondary: '#8B5CF6', // Violet 500
            }
        },
    },
    plugins: [],
}
