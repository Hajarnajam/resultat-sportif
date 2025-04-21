import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import axios from 'axios';
import { FaMedal, FaSearch, FaVenus, FaMars, FaVenusMars, FaArrowLeft } from 'react-icons/fa';

function ParticipantList() {
  const { competitionName, courseName } = useParams();
  const navigate = useNavigate();
  const [participants, setParticipants] = useState([]);
  const [courses, setCourses] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [genderFilter, setGenderFilter] = useState('all');
  

  const convertTimeToSeconds = (time) => {
    if (!time || time === 0 || time === "00s" || time === "Non parti" || time === "00:00:00.00") {
      return Infinity; 
    }
    if (typeof time === 'number') return time;
    
    const parts = time.split(':');
    if (parts.length === 3) {
      return parseInt(parts[0]) * 3600 + parseInt(parts[1]) * 60 + parseFloat(parts[2]);
    } else if (parts.length === 2) {
      return parseInt(parts[0]) * 60 + parseFloat(parts[1]);
    }
    return Infinity;
  };

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        
        const [participantsRes, coursesRes] = await Promise.all([
          axios.get(`https://resultat-sportif.onrender.com/competitions/${competitionName}/courses/${courseName}/participants`),
          axios.get(`https://resultat-sportif.onrender.com/competitions/${competitionName}/courses`)
        ]);

        setParticipants(participantsRes.data);
        setCourses(coursesRes.data);
        setLoading(false);
      } catch (err) {
        console.error('Erreur:', err);
        setError(err);
        setLoading(false);
      }
    };

    fetchData();
  }, [competitionName, courseName]);

  const sortedParticipants = [...participants].sort((a, b) => {
    const aTime = convertTimeToSeconds(a.Temps);
    const bTime = convertTimeToSeconds(b.Temps);

    if (aTime === Infinity && bTime === Infinity) return 0;
    if (aTime === Infinity) return 1;
    if (bTime === Infinity) return -1;
    return aTime - bTime;
  });

  const participantsWithPosition = sortedParticipants.map((participant, index) => {
    const isNonParti = convertTimeToSeconds(participant.Temps) === Infinity;
    const position = isNonParti 
      ? sortedParticipants.filter(p => convertTimeToSeconds(p.Temps) !== Infinity).length + 1
      : index + 1;
    
    return { 
      ...participant, 
      position,
      displayTime: participant.Temps === "00:00:00.00" ? "Non parti" : participant.Temps
    };
  });

  const filteredParticipants = participantsWithPosition.filter(participant => {
    const matchesSearch = ['Nom', 'Prénom', 'Club'].some(field =>
      String(participant[field] || '').toLowerCase().includes(searchTerm.toLowerCase())
    );
    
    const matchesGender = genderFilter === 'all' || 
      (genderFilter === 'male' && participant.Sexe === 'M') || 
      (genderFilter === 'female' && participant.Sexe === 'F');
    
    return matchesSearch && matchesGender;
  });

  if (loading) return (
    <div className="loading-container">
      <div className="spinner"></div>
      <p>Chargement des résultats...</p>
    </div>
  );

  if (error) return (
    <div className="error-container">
      <p>Erreur lors du chargement des données</p>
      <button onClick={() => window.location.reload()}>Réessayer</button>
    </div>
  );

  return (
    <div className="results-container">
      <div className="results-header">
        <button 
          className="back-icon"
          onClick={() => navigate(-1)}
          title="Retour"
        >
          <FaArrowLeft />
        </button> 
        <h1 className="event-title">{courseName}</h1>
        <h2 className="competition-subtitle">{competitionName}</h2>
      </div>

      <div className="search-section">
        <div className="search-box">
          <FaSearch className="search-icon" />
          <input
            type="text"
            placeholder="Rechercher un athlète..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>
      </div>

      <div className="event-navigation">
        <h3>Autres épreuves :</h3>
        <div className="event-buttons">
          {courses.map((course, i) => (
            <button
              key={i}
              onClick={() => navigate(`/competitions/${competitionName}/courses/${course.name}`)}
              className={course.name === courseName ? 'active' : ''}
            >
              {course.name}
            </button>
          ))}
        </div>
      </div>

      <div className="gender-filter">
        <button 
          onClick={() => setGenderFilter('all')} 
          className={genderFilter === 'all' ? 'active' : ''}
        >
          <FaVenusMars /> Tous
        </button>
        <button 
          onClick={() => setGenderFilter('male')} 
          className={genderFilter === 'male' ? 'active' : ''}
        >
          <FaMars /> Hommes
        </button>
        <button 
          onClick={() => setGenderFilter('female')} 
          className={genderFilter === 'female' ? 'active' : ''}
        >
          <FaVenus /> Femmes
        </button>
      </div>

      <div className="results-table-container">
        <table className="results-table">
          <thead>
            <tr>
              <th className="position-col">Position</th>
              <th className="athlete-col">Athlète</th>
              <th className="club-col">Club</th>
              <th className="time-col">Temps</th>
            </tr>
          </thead>
          <tbody>
            {filteredParticipants.length > 0 ? (
              filteredParticipants.map((participant, index) => (
                <tr key={participant._id || index} className={index % 2 === 0 ? 'even-row' : 'odd-row'}>
                  <td className="position-cell">
                    {participant.position <= 3 ? (
                      <div className={`medal medal-${participant.position}`}>
                        <FaMedal />
                      </div>
                    ) : (
                      participant.position
                    )}
                  </td>
                  <td className="athlete-cell">
                    <div className="athlete-name">
                      {participant.Prénom} {participant.Nom}
                      <span className="gender-icon">
                        {participant.Sexe === 'M' ? <FaMars /> : <FaVenus />}
                      </span>
                    </div>
                  </td>
                  <td className="club-cell">{participant.Club || '-'}</td>
                  <td className={`time-cell ${participant.displayTime === "Non parti" ? 'dnf' : ''}`}>
                    {participant.displayTime}
                  </td>
                </tr>
              ))
            ) : (
              <tr className="no-results">
                <td colSpan="4">Aucun participant trouvé</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default ParticipantList;
