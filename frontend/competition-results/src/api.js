import axios from 'axios';


// Base URL for backend
const API_URL = 'https://resultat-sportif.onrender.com/competitions';

// Fetch all competitions
export const getCompetitions = async () => {
  try {
    const response = await axios.get(API_URL);
    return response.data;  // List of competitions
  } catch (error) {
    console.error('Error fetching competitions:', error);
    return [];
  }
};

// Fetch courses for a specific competition
export const getCourses = async (competitionName) => {
  try {
    const response = await axios.get(`${API_URL}/${competitionName}/courses`);
    return response.data;  // List of courses for a given competition
  } catch (error) {
    console.error(`Error fetching courses for ${competitionName}:`, error);
    return [];
  }
};

// Fetch participants for a specific course in a specific competition
export const getParticipants = async (competitionName, courseName) => {
  try {
    const response = await axios.get(`${API_URL}/${competitionName}/courses/${courseName}/participants`);
    return response.data;  // List of participants
  } catch (error) {
    console.error(`Error fetching participants for ${courseName} in ${competitionName}:`, error);
    return [];
  }
};